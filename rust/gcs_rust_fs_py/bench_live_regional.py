"""Focused live GCS benchmark on princer-us-west4a: python_aiohttp vs rust_http (1 MiB & 4 MiB)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import resource
import statistics
import time
import uuid

import gcsfs


def percentile(values: list[float], p: float) -> float:
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return s[idx]


def cpu_ms() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return (ru.ru_utime + ru.ru_stime) * 1000.0


def run_benchmark():
    bucket = os.environ.get("GCSFS_TEST_BUCKET", "princer-flat")
    run_id = uuid.uuid4().hex[:8]
    prefix = f"{bucket}/poc-rust-bench-{run_id}"

    configs = [
        ("python_aiohttp", False, "http"),
        ("rust_http", True, "http"),
    ]

    results = []

    for label, use_rust, transport in configs:
        fs = gcsfs.GCSFileSystem(
            token="google_default",
            use_rust_fs=use_rust,
            rust_transport=transport,
            cache_timeout=0,
        )

        # Warmup connection & StorageLayout cache
        warm_path = f"{prefix}/warm-{label}.bin"
        fs.pipe_file(warm_path, b"warmup-payload")
        fs.info(warm_path)
        fs.cat_file(warm_path)

        # 1. info benchmark (30 iterations, uncached)
        info_lats = []
        c0 = cpu_ms()
        for _ in range(30):
            t0 = time.perf_counter()
            info = fs.info(warm_path)
            assert info["size"] == 14
            info_lats.append((time.perf_counter() - t0) * 1000.0)
        c_delta = cpu_ms() - c0
        results.append(
            {
                "backend": label,
                "op": "info",
                "size_bytes": 14,
                "iters": 30,
                "p50_ms": round(statistics.median(info_lats), 2),
                "p99_ms": round(percentile(info_lats, 99), 2),
                "mean_ms": round(statistics.mean(info_lats), 2),
                "throughput_mb_s": None,
                "cpu_ms_per_op": round(c_delta / 30.0, 3),
            }
        )

        # 2. write (pipe_file) & read (cat_file) for 1 MiB and 4 MiB
        for size, iters in [
            (1 * 1024 * 1024, 20),
            (4 * 1024 * 1024, 20),
        ]:
            payload = os.urandom(size)
            size_mib = size / (1024 * 1024)

            # 2a. Single-stream write (pipe_file) to distinct keys per iteration
            w_lats = []
            c0 = cpu_ms()
            last_path = None
            for i in range(iters):
                path = f"{prefix}/obj-{label}-{size}-{i}.bin"
                last_path = path
                t0 = time.perf_counter()
                fs.pipe_file(path, payload)
                w_lats.append((time.perf_counter() - t0) * 1000.0)
            w_cpu = (cpu_ms() - c0) / iters
            w_p50 = statistics.median(w_lats)
            results.append(
                {
                    "backend": label,
                    "op": "write (pipe_file)",
                    "size_bytes": size,
                    "iters": iters,
                    "p50_ms": round(w_p50, 2),
                    "p99_ms": round(percentile(w_lats, 99), 2),
                    "mean_ms": round(statistics.mean(w_lats), 2),
                    "throughput_mb_s": round(size_mib / (w_p50 / 1000.0), 2),
                    "cpu_ms_per_op": round(w_cpu, 3),
                }
            )

            # 2b. Single-stream read (cat_file)
            r_lats = []
            c0 = cpu_ms()
            for _ in range(iters):
                t0 = time.perf_counter()
                data = fs.cat_file(last_path)
                assert len(data) == size
                r_lats.append((time.perf_counter() - t0) * 1000.0)
            r_cpu = (cpu_ms() - c0) / iters
            r_p50 = statistics.median(r_lats)
            results.append(
                {
                    "backend": label,
                    "op": "read (cat_file)",
                    "size_bytes": size,
                    "iters": iters,
                    "p50_ms": round(r_p50, 2),
                    "p99_ms": round(percentile(r_lats, 99), 2),
                    "mean_ms": round(statistics.mean(r_lats), 2),
                    "throughput_mb_s": round(size_mib / (r_p50 / 1000.0), 2),
                    "cpu_ms_per_op": round(r_cpu, 3),
                }
            )

            # 2c. 16-thread concurrent read (cat_file, 32 files across 16 threads)
            mt_iters = 32
            c0 = cpu_ms()
            t_wall0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=16) as pool:
                futs = [pool.submit(fs.cat_file, last_path) for _ in range(mt_iters)]
                for f in futs:
                    assert len(f.result()) == size
            wall_s = time.perf_counter() - t_wall0
            mt_r_cpu = (cpu_ms() - c0) / mt_iters
            results.append(
                {
                    "backend": label,
                    "op": "read_mt16 (cat_file x32)",
                    "size_bytes": size,
                    "iters": mt_iters,
                    "p50_ms": round((wall_s * 1000.0), 2),
                    "p99_ms": round((wall_s * 1000.0), 2),
                    "mean_ms": round((wall_s * 1000.0) / mt_iters, 2),
                    "throughput_mb_s": round((size_mib * mt_iters) / wall_s, 2),
                    "cpu_ms_per_op": round(mt_r_cpu, 3),
                }
            )

            # 2d. 16-thread concurrent write (pipe_file, 32 distinct keys across 16 threads)
            c0 = cpu_ms()
            t_wall0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=16) as pool:
                futs = [
                    pool.submit(
                        fs.pipe_file,
                        f"{prefix}/mt-obj-{label}-{size}-{i}.bin",
                        payload,
                    )
                    for i in range(mt_iters)
                ]
                for f in futs:
                    f.result()
            wall_w_s = time.perf_counter() - t_wall0
            mt_w_cpu = (cpu_ms() - c0) / mt_iters
            results.append(
                {
                    "backend": label,
                    "op": "write_mt16 (pipe_file x32)",
                    "size_bytes": size,
                    "iters": mt_iters,
                    "p50_ms": round((wall_w_s * 1000.0), 2),
                    "p99_ms": round((wall_w_s * 1000.0), 2),
                    "mean_ms": round((wall_w_s * 1000.0) / mt_iters, 2),
                    "throughput_mb_s": round((size_mib * mt_iters) / wall_w_s, 2),
                    "cpu_ms_per_op": round(mt_w_cpu, 3),
                }
            )

    # Cleanup benchmark objects
    cleanup_fs = gcsfs.GCSFileSystem(token="google_default")
    try:
        cleanup_fs.rm(prefix, recursive=True)
    except Exception:
        pass

    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(
        json.dumps(
            {"bucket": bucket, "results": results, "max_rss_kb": rss_kb}, indent=2
        )
    )


if __name__ == "__main__":
    run_benchmark()
