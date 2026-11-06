#!/usr/bin/env python3
"""Four-Stack 20 GiB Live GCS Benchmark Suite (`US-WEST4`, `Transport::Http`).

Benchmarks:
- Scenario 1 (`s1_python`): Pure Python `gcsfs` (`use_rust_fs=False`)
- Scenario 2 (`s2_pyo3_direct`): `gcsfs` + Direct PyO3 (`CachedFs<GcsFs>`, Mode 1 readahead, zero `fsspecIO`)
- Scenario 3 (`s3_uds_grpc_aio`): `gcsfs` + Pure Python `grpc.aio` UDS client -> `gcsfs-sidecar`
- Scenario 4 (`s4_pyo3_remotefs`): `gcsfs` + PyO3 `RemoteFs` UDS client -> `gcsfs-sidecar`
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any

import gcsfs

SIDECAR_BIN = "/home/princer_google_com/dev/gcs-rust-fs/target/release/gcsfs-sidecar"
DEFAULT_UDS_SOCK = "/tmp/gcsfs-sidecar-bench.sock"

SCENARIO_LABELS = {
    "s1_python": "Scenario 1: Pure Python gcsfs (Stack 2)",
    "s2_pyo3_direct": "Scenario 2: Direct PyO3 CachedFs<GcsFs> (Stack 3)",
    "s3_uds_grpc_aio": "Scenario 3: Python grpc.aio over UDS Sidecar (Stack 4a)",
    "s4_pyo3_remotefs": "Scenario 4: PyO3 RemoteFs over UDS Sidecar (Stack 4b)",
}


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def generate_random_offsets(file_size: int, read_size: int, count: int) -> list[int]:
    """Deterministic SplitMix64 4 MiB-aligned offsets matching `bench_live_readahead.rs`."""
    align = 4 * 1024 * 1024
    slots = max(1, (file_size - read_size) // align)
    state = 0x9E37_79B9_7F4A_7C15
    used: set[int] = set()
    offsets: list[int] = []
    mask = (1 << 64) - 1
    while len(offsets) < count:
        state = (state + 0x9E37_79B9_7F4A_7C15) & mask
        z = state
        z = ((z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9) & mask
        z = ((z ^ (z >> 27)) * 0x94D0_49BB_1331_11EB) & mask
        z = z ^ (z >> 31)
        slot = z % slots
        if slot not in used:
            used.add(slot)
            offsets.append(int(slot * align))
    return offsets


def shard_path_for_worker(base_path: str, worker_idx: int, total_workers: int) -> str:
    if total_workers <= 1:
        return base_path
    if base_path.endswith(".bin"):
        stem = base_path[:-4]
        return f"{stem}_{worker_idx:02d}.bin"
    return f"{base_path}_{worker_idx:02d}"


def make_fs(scenario: str, uds_sock: str) -> gcsfs.GCSFileSystem:
    if scenario == "s1_python":
        return gcsfs.GCSFileSystem(
            use_rust_fs=False,
            use_uds_grpc=False,
            cache_timeout=0,
        )
    if scenario == "s2_pyo3_direct":
        return gcsfs.GCSFileSystem(
            use_rust_fs=True,
            rust_transport="http",
            rust_uds_path=None,
            cache_timeout=0,
        )
    if scenario == "s3_uds_grpc_aio":
        return gcsfs.GCSFileSystem(
            use_rust_fs=False,
            use_uds_grpc=True,
            uds_socket_path=uds_sock,
            cache_timeout=0,
        )
    if scenario == "s4_pyo3_remotefs":
        return gcsfs.GCSFileSystem(
            use_rust_fs=True,
            rust_transport="http",
            rust_uds_path=uds_sock,
            cache_timeout=0,
        )
    raise ValueError(f"Unknown scenario: {scenario}")


class SidecarManager:
    """Manages the out-of-process `gcsfs-sidecar` daemon lifecycle."""

    def __init__(self, uds_sock: str, http_pool_size: int = 48) -> None:
        self.uds_sock = uds_sock
        self.http_pool_size = http_pool_size
        self.proc: subprocess.Popen[bytes] | None = None
        self.readahead_enabled: bool | None = None

    def start(self, readahead: bool) -> None:
        if self.proc is not None and self.proc.poll() is None:
            if self.readahead_enabled == readahead:
                return
            self.stop()

        if os.path.exists(self.uds_sock):
            try:
                os.unlink(self.uds_sock)
            except OSError:
                pass

        cmd = [
            SIDECAR_BIN,
            "--uds",
            self.uds_sock,
            "--transport",
            "http",
            "--http-pool-size",
            str(self.http_pool_size),
        ]
        if readahead:
            cmd.append("--readahead")

        print(f"  [sidecar] starting: {' '.join(cmd)}", flush=True)
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self.readahead_enabled = readahead
        deadline = time.monotonic() + 15.0
        while not os.path.exists(self.uds_sock):
            if self.proc.poll() is not None:
                err = (
                    self.proc.stderr.read().decode("utf-8", errors="replace")
                    if self.proc.stderr
                    else ""
                )
                raise RuntimeError(f"gcsfs-sidecar exited early: {err}")
            if time.monotonic() > deadline:
                raise TimeoutError("Timed out waiting for gcsfs-sidecar UDS socket")
            time.sleep(0.05)
        time.sleep(0.2)

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()
            self.proc = None
            self.readahead_enabled = None
        if os.path.exists(self.uds_sock):
            try:
                os.unlink(self.uds_sock)
            except OSError:
                pass


def run_part1_info(
    scenario: str, path: str, uds_sock: str, iterations: int = 30
) -> dict[str, Any]:
    fs = make_fs(scenario, uds_sock)
    # Warm up connection/channel once
    _ = fs.info(path)
    latencies_ms: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        info = fs.info(path)
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        assert info["size"] > 0
    s = sorted(latencies_ms)
    mean_ms = sum(s) / len(s)
    res = {
        "iterations": iterations,
        "mean_ms": round(mean_ms, 2),
        "min_ms": round(s[0], 2),
        "p50_ms": round(percentile(s, 50.0), 2),
        "p90_ms": round(percentile(s, 90.0), 2),
        "p99_ms": round(percentile(s, 99.0), 2),
        "max_ms": round(s[-1], 2),
    }
    print(
        f"    Part 1 [info 30x] ({scenario}): mean={res['mean_ms']}ms, "
        f"min={res['min_ms']}ms, p50={res['p50_ms']}ms, p90={res['p90_ms']}ms, p99={res['p99_ms']}ms",
        flush=True,
    )
    return res


def run_part2_random_read(
    scenario: str,
    path: str,
    uds_sock: str,
    read_size: int = 1024 * 1024,
    num_reads: int = 50,
) -> dict[str, Any]:
    fs = make_fs(scenario, uds_sock)
    info = fs.info(path)
    file_size = int(info["size"])
    offsets = generate_random_offsets(file_size, read_size, num_reads)

    latencies_ms: list[float] = []
    total_bytes = 0
    with fs.open(path, "rb") as f:
        t_start = time.perf_counter()
        for off in offsets:
            t0 = time.perf_counter()
            f.seek(off)
            chunk = f.read(read_size)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            if len(chunk) != read_size:
                raise RuntimeError(f"Short random read: got {len(chunk)}, expected {read_size}")
            total_bytes += len(chunk)
        wall_s = time.perf_counter() - t_start

    s = sorted(latencies_ms)
    mb_s = (total_bytes / 1e6) / wall_s
    mib_s = (total_bytes / (1024.0 * 1024.0)) / wall_s
    iops = num_reads / wall_s
    res = {
        "reads": num_reads,
        "read_size_bytes": read_size,
        "wall_s": round(wall_s, 3),
        "p50_ms": round(percentile(s, 50.0), 2),
        "p90_ms": round(percentile(s, 90.0), 2),
        "p99_ms": round(percentile(s, 99.0), 2),
        "iops": round(iops, 2),
        "mb_s": round(mb_s, 2),
        "mib_s": round(mib_s, 2),
    }
    print(
        f"    Part 2 [1 MiB RandRead 50x] ({scenario}): p50={res['p50_ms']}ms, "
        f"p90={res['p90_ms']}ms, p99={res['p99_ms']}ms, IOPS={res['iops']}, "
        f"{res['mb_s']} MB/s ({res['mib_s']} MiB/s)",
        flush=True,
    )
    return res


def _seq_worker_fn(
    scenario: str,
    shard_path: str,
    uds_sock: str,
    chunk_size: int,
    max_duration_secs: float,
    barrier: Any,
) -> tuple[int, float, list[float]]:
    fs = make_fs(scenario, uds_sock)
    f = fs.open(shard_path, "rb", block_size=chunk_size)
    try:
        barrier.wait()
        t_start = time.perf_counter()
        total_bytes = 0
        chunk_latencies_ms: list[float] = []
        while True:
            now = time.perf_counter()
            if now - t_start >= max_duration_secs:
                break
            t0 = now
            data = f.read(chunk_size)
            t1 = time.perf_counter()
            if not data:
                break
            total_bytes += len(data)
            chunk_latencies_ms.append((t1 - t0) * 1000.0)
        elapsed = time.perf_counter() - t_start
        return total_bytes, elapsed, chunk_latencies_ms
    finally:
        f.close()


def _mp_worker_entry(args: tuple[Any, ...]) -> tuple[int, float, list[float]]:
    scenario, shard_path, uds_sock, chunk_size, max_duration_secs, barrier = args
    return _seq_worker_fn(
        scenario, shard_path, uds_sock, chunk_size, max_duration_secs, barrier
    )


_MP_BARRIER: Any = None


def _init_mp_pool(barrier: Any) -> None:
    global _MP_BARRIER
    _MP_BARRIER = barrier
    os.environ["GCSFS_RUST_WORKER_THREADS"] = "4"
    os.environ["GCS_RUST_FS_HTTP_POOL_SIZE"] = "4"
    os.environ["GCS_RUST_FS_UDS_POOL_SIZE"] = "2"
    os.environ["GCS_RUST_FS_RA_MAX_READAHEAD_MB"] = "64"
    os.environ["GCS_RUST_FS_RA_MAX_INFLIGHT"] = "4"


def _mp_pool_task(args: tuple[str, str, str, int, float]) -> tuple[int, float, list[float]]:
    scenario, shard_path, uds_sock, chunk_size, max_duration_secs = args
    return _seq_worker_fn(
        scenario, shard_path, uds_sock, chunk_size, max_duration_secs, _MP_BARRIER
    )


def run_part3_sequential(
    scenario: str,
    base_path: str,
    uds_sock: str,
    concurrency_spec: str,
    max_duration_secs: float = 120.0,
    chunk_size: int = 8 * 1024 * 1024,
) -> dict[str, Any]:
    if concurrency_spec == "1":
        num_workers = 1
        mode = "thread"
    elif concurrency_spec.endswith("T"):
        num_workers = int(concurrency_spec[:-1])
        mode = "thread"
    elif concurrency_spec.endswith("P"):
        num_workers = int(concurrency_spec[:-1])
        mode = "process"
    else:
        num_workers = int(concurrency_spec)
        mode = "thread"

    shards = [shard_path_for_worker(base_path, i, num_workers) for i in range(num_workers)]

    if mode == "thread":
        barrier = threading.Barrier(num_workers)
        with ThreadPoolExecutor(max_workers=num_workers) as ex:
            futs = [
                ex.submit(
                    _seq_worker_fn,
                    scenario,
                    shards[i],
                    uds_sock,
                    chunk_size,
                    max_duration_secs,
                    barrier,
                )
                for i in range(num_workers)
            ]
            results = [fut.result() for fut in futs]
    else:
        ctx = mp.get_context("spawn")
        barrier = ctx.Barrier(num_workers)
        tasks = [
            (scenario, shards[i], uds_sock, chunk_size, max_duration_secs)
            for i in range(num_workers)
        ]
        with ctx.Pool(
            processes=num_workers, initializer=_init_mp_pool, initargs=(barrier,)
        ) as pool:
            results = pool.map(_mp_pool_task, tasks)

    total_bytes = sum(r[0] for r in results)
    wall_s = max(r[1] for r in results)
    all_lats: list[float] = []
    for r in results:
        all_lats.extend(r[2])
    all_lats.sort()

    target_bytes = num_workers * 20 * (1024**3)
    total_gib = total_bytes / (1024.0**3)
    mb_s = (total_bytes / 1e6) / wall_s if wall_s > 0 else 0.0
    mib_s = (total_bytes / (1024.0 * 1024.0)) / wall_s if wall_s > 0 else 0.0
    capped = total_bytes < target_bytes

    res = {
        "concurrency": concurrency_spec,
        "workers": num_workers,
        "mode": mode,
        "wall_s": round(wall_s, 2),
        "total_bytes": total_bytes,
        "total_gib": round(total_gib, 2),
        "target_gib": num_workers * 20,
        "time_capped": capped,
        "chunk_p50_ms": round(percentile(all_lats, 50.0), 2),
        "chunk_p90_ms": round(percentile(all_lats, 90.0), 2),
        "mb_s": round(mb_s, 2),
        "mib_s": round(mib_s, 2),
    }
    cap_note = " [TIME-CAPPED @ 120s]" if capped else ""
    print(
        f"    Part 3 [SeqRead {concurrency_spec}] ({scenario}): "
        f"{res['total_gib']}/{res['target_gib']} GiB in {res['wall_s']}s -> "
        f"{res['mb_s']} MB/s ({res['mib_s']} MiB/s), "
        f"chunk p50={res['chunk_p50_ms']}ms, p90={res['chunk_p90_ms']}ms{cap_note}",
        flush=True,
    )
    return res


def run_part4_write(
    scenario: str,
    base_path: str,
    uds_sock: str,
    write_size_mb: int = 200,
    chunk_size: int = 8 * 1024 * 1024,
    iterations: int = 5,
) -> dict[str, Any]:
    fs = make_fs(scenario, uds_sock)
    write_path = (
        f"{base_path[:-4]}_write_{write_size_mb}MB.bin"
        if base_path.endswith(".bin")
        else f"{base_path}_write_{write_size_mb}MB.bin"
    )
    total_bytes = write_size_mb * 1024 * 1024
    payload = bytes((i % 251 for i in range(1024 * 1024))) * write_size_mb
    chunks = [
        payload[offset : min(offset + chunk_size, total_bytes)]
        for offset in range(0, total_bytes, chunk_size)
    ]

    stream_times: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        with fs.open(write_path, "wb", block_size=chunk_size) as wf:
            for ch in chunks:
                wf.write(ch)
        stream_times.append(time.perf_counter() - t0)

    pipe_times: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fs.pipe_file(write_path, payload)
        pipe_times.append(time.perf_counter() - t0)

    def _summarize(times: list[float]) -> dict[str, Any]:
        s = sorted(times)
        p50_s = percentile(s, 50.0)
        return {
            "iterations": iterations,
            "mean_s": round(sum(s) / len(s), 3),
            "p50_s": round(p50_s, 3),
            "p90_s": round(percentile(s, 90.0), 3),
            "mb_s_p50": round((total_bytes / 1e6) / p50_s, 2),
            "mib_s_p50": round((total_bytes / (1024.0 * 1024.0)) / p50_s, 2),
        }

    res = {
        "write_size_mib": write_size_mb,
        "streaming_write": _summarize(stream_times),
        "pipe_file": _summarize(pipe_times),
    }
    sw = res["streaming_write"]
    pf = res["pipe_file"]
    print(
        f"    Part 4 [200 MiB Write 5x] ({scenario}): "
        f"stream p50={sw['p50_s']}s ({sw['mb_s_p50']} MB/s) | "
        f"pipe_file p50={pf['p50_s']}s ({pf['mb_s_p50']} MB/s)",
        flush=True,
    )
    return res


def main() -> None:
    parser = argparse.ArgumentParser(description="Four-Stack 20 GiB GCS Benchmark Runner")
    parser.add_argument(
        "--path",
        default="gs://princer-flat/bench/bench_20GB.bin",
        help="Target 20 GiB GCS object path",
    )
    parser.add_argument(
        "--uds-sock",
        default=DEFAULT_UDS_SOCK,
        help="Unix Domain Socket path for gcsfs-sidecar",
    )
    parser.add_argument(
        "--max-duration-secs",
        type=float,
        default=120.0,
        help="Per-test duration cap in seconds for Part 3 sequential reads",
    )
    parser.add_argument(
        "--scenarios",
        default="s1_python,s2_pyo3_direct,s3_uds_grpc_aio,s4_pyo3_remotefs",
        help="Comma-separated scenario IDs",
    )
    parser.add_argument(
        "--parts",
        default="1,2,3,4",
        help="Comma-separated benchmark parts (1,2,3,4)",
    )
    parser.add_argument(
        "--concurrencies",
        default="1,24T,24P,48T,48P",
        help="Comma-separated concurrency specs for Part 3",
    )
    parser.add_argument(
        "--json-out",
        default="benchmarks/results_four_stacks_20gb.json",
        help="Path to write JSON results",
    )
    args = parser.parse_args()

    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    parts = {int(p.strip()) for p in args.parts.split(",") if p.strip()}
    concurrencies = [c.strip() for c in args.concurrencies.split(",") if c.strip()]

    needs_sidecar = any(s in ("s3_uds_grpc_aio", "s4_pyo3_remotefs") for s in scenarios)
    sidecar = SidecarManager(args.uds_sock, http_pool_size=48)

    all_results: dict[str, Any] = {}
    if os.path.exists(args.json_out):
        try:
            with open(args.json_out, "r", encoding="utf-8") as f:
                all_results = json.load(f)
        except Exception:
            all_results = {}

    def _save() -> None:
        os.makedirs(os.path.dirname(os.path.abspath(args.json_out)), exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2)

    try:
        if 1 in parts:
            print("\n=== PART 1: Single-Worker Uncached info() Latency (30 iterations) ===", flush=True)
            if needs_sidecar:
                sidecar.start(readahead=False)
            for sc in scenarios:
                all_results.setdefault(sc, {})["part1_info"] = run_part1_info(
                    sc, args.path, args.uds_sock
                )
                _save()

        if any(p in parts for p in (2, 3, 4)) and needs_sidecar:
            sidecar.start(readahead=True)

        if 2 in parts:
            print("\n=== PART 2: Single-Worker 1 MiB Random Read (50 reads) ===", flush=True)
            for sc in scenarios:
                all_results.setdefault(sc, {})["part2_random_read"] = run_part2_random_read(
                    sc, args.path, args.uds_sock
                )
                _save()

        if 3 in parts:
            print(
                f"\n=== PART 3: 20 GiB Sequential Read (8 MiB chunks, cap={args.max_duration_secs}s) ===",
                flush=True,
            )
            for sc in scenarios:
                sc_p3 = all_results.setdefault(sc, {}).setdefault("part3_sequential", {})
                for conc in concurrencies:
                    sc_p3[conc] = run_part3_sequential(
                        sc,
                        args.path,
                        args.uds_sock,
                        conc,
                        max_duration_secs=args.max_duration_secs,
                    )
                    _save()

        if 4 in parts:
            print("\n=== PART 4: Single-Worker 200 MiB Write (5 iterations) ===", flush=True)
            for sc in scenarios:
                all_results.setdefault(sc, {})["part4_write"] = run_part4_write(
                    sc, args.path, args.uds_sock
                )
                _save()
    finally:
        sidecar.stop()

    print(f"\nSaved complete benchmark results to {args.json_out}", flush=True)


if __name__ == "__main__":
    main()

