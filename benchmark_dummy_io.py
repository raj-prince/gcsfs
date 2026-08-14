"""
Single-Threaded Read Path Dummy IO Benchmark Script with Network Latency Simulation (TTFB & Bandwidth).

Usage:
    python benchmark_dummy_io.py [--iterations N] [--size-gb GB] [--ttfb-ms MS] [--bandwidth-mbps MBPS] [--profile]
"""

import argparse
import asyncio
import cProfile
import logging
import os
import pstats
import resource
import sys
import threading
import time

# Ensure local repository root is prioritized in sys.path
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import gcsfs
from gcsfs.extended_gcsfs import BucketType

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logging.getLogger("gcsfs").setLevel(logging.ERROR)


class ResourceTracker:
    """Background sampling thread to track peak RSS Memory and CPU % during a test round."""
    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.max_rss_bytes = 0
        self.max_cpu_percent = 0.0
        self._stop_event = threading.Event()
        self._thread = None
        self._start_cpu_time = 0.0
        self._start_wall_time = 0.0
        self._end_cpu_time = 0.0
        self._end_wall_time = 0.0

    def start(self):
        self.max_rss_bytes = 0
        self.max_cpu_percent = 0.0
        self._stop_event.clear()
        self._start_cpu_time = time.process_time()
        self._start_wall_time = time.perf_counter()
        if HAS_PSUTIL:
            self._proc = psutil.Process()
            self._proc.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()

    def _monitor(self):
        while not self._stop_event.is_set():
            try:
                if HAS_PSUTIL:
                    mem = self._proc.memory_info().rss
                    cpu = self._proc.cpu_percent(interval=None)
                    if mem > self.max_rss_bytes:
                        self.max_rss_bytes = mem
                    if cpu > self.max_cpu_percent:
                        self.max_cpu_percent = cpu
                else:
                    rusage = resource.getrusage(resource.RUSAGE_SELF)
                    mem = rusage.ru_maxrss * 1024
                    if mem > self.max_rss_bytes:
                        self.max_rss_bytes = mem
            except Exception:
                pass
            self._stop_event.wait(self.interval)

    def stop(self):
        self._end_cpu_time = time.process_time()
        self._end_wall_time = time.perf_counter()
        self._stop_event.set()
        if self._thread:
            self._thread.join()

        rusage_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        if rusage_bytes > self.max_rss_bytes:
            self.max_rss_bytes = rusage_bytes

    @property
    def peak_mem_mb(self) -> float:
        return self.max_rss_bytes / (1024 * 1024)

    @property
    def avg_cpu_percent(self) -> float:
        wall = self._end_wall_time - self._start_wall_time
        if wall <= 0:
            return 0.0
        return ((self._end_cpu_time - self._start_cpu_time) / wall) * 100.0


def format_speed(bytes_per_sec: float) -> str:
    """Formats transfer speed into MB/s or GB/s."""
    mb_s = bytes_per_sec / (1024 * 1024)
    if mb_s >= 1024:
        return f"{mb_s / 1024:.2f} GB/s"
    return f"{mb_s:.2f} MB/s"


def run_benchmark(
    iterations: int = 1,
    file_size_gb: float = 2.0,
    chunk_sizes_mb: list[int] | None = None,
    ttfb_ms: float = 0.0,
    bandwidth_mbps: float = 0.0,
    byte_latency_ns: float = 0.0,
    profile_enabled: bool = False,
):
    os.environ["GCSFS_DUMMY_IO"] = "1"
    if ttfb_ms > 0:
        os.environ["GCSFS_DUMMY_TTFB_MS"] = str(ttfb_ms)
    if bandwidth_mbps > 0:
        os.environ["GCSFS_DUMMY_BANDWIDTH_MBPS"] = str(bandwidth_mbps)
    if byte_latency_ns > 0:
        os.environ["GCSFS_DUMMY_BYTE_LATENCY_NS"] = str(byte_latency_ns)

    fs = gcsfs.GCSFileSystem(token="anon", dummy_io=True)
    fs.dummy_ttfb_s = ttfb_ms / 1000.0
    fs.dummy_bandwidth_bps = bandwidth_mbps * 1024 * 1024
    fs.dummy_byte_latency_s = byte_latency_ns / 1e9

    file_size_bytes = int(file_size_gb * 1024 * 1024 * 1024)
    fake_info_dict = {
        "name": "demo-bucket/test-virtual-file.bin",
        "size": file_size_bytes,
        "type": "file",
        "bucket": "demo-bucket",
        "storageClass": "STANDARD",
    }

    async def fake_info(path, **kwargs):
        return fake_info_dict

    fs._info = fake_info
    fs.info = lambda path, **kwargs: fake_info_dict

    path = "demo-bucket/test-virtual-file.bin"
    if chunk_sizes_mb is None:
        chunk_sizes_mb = [1, 4, 5, 8, 16, 64]

    modes = [
        ("Standard GCSFile (Regional)", BucketType.NON_HIERARCHICAL),
    ]

    configs = [
        ("Direct Buffering (No Prefetch)", False, False, "none"),
        ("Current BackgroundPrefetcher", True, False, "none"),
        ("ZeroCopySlabPrefetcher (Fixed Slab Pool + Parallel memoryview)", True, True, "none"),
    ]

    tracker = ResourceTracker(interval=0.03)

    latency_desc = []
    if ttfb_ms > 0:
        latency_desc.append(f"TTFB: {ttfb_ms:.1f} ms")
    if bandwidth_mbps > 0:
        latency_desc.append(f"Bandwidth: {bandwidth_mbps:.1f} MB/s")
    if byte_latency_ns > 0:
        latency_desc.append(f"Per-Byte Latency: {byte_latency_ns:.2f} ns")
    if not latency_desc:
        latency_desc = ["Zero Network Latency (Pure CPU)"]

    print("=" * 115, flush=True)
    print(" GCSFS SINGLE-THREADED READ PATH THROUGHPUT BENCHMARK & CPU PROFILER ", flush=True)
    print("=" * 115, flush=True)
    print(f" Virtual File Size: {file_size_gb:.1f} GB | Iterations: {iterations} | Simulation: {', '.join(latency_desc)}", flush=True)
    print("=" * 115, flush=True)

    results = {}

    for mode_name, bucket_type in modes:
        fs._sync_lookup_bucket_type = lambda bucket, _bt=bucket_type: _bt

        print(f"\n>>> Mode: {mode_name}", flush=True)
        print("-" * 115, flush=True)

        for cfg_label, use_prefetch, use_slab, prefetch_cache_type in configs:
            print(f"\n  [ Configuration: {cfg_label} ]", flush=True)

            for chunk_mb in chunk_sizes_mb:
                chunk_bytes = chunk_mb * 1024 * 1024

                open_kwargs = {
                    "block_size": chunk_bytes,
                    "slab_size": max(1024 * 1024, chunk_bytes),
                    "cache_type": prefetch_cache_type,
                    "use_experimental_adaptive_prefetching": use_prefetch,
                    "use_slab_prefetcher": use_slab,
                    "dummy_io": True,
                    "size": file_size_bytes,
                }

                prof = cProfile.Profile() if profile_enabled else None

                with fs.open(path, "rb", **open_kwargs) as f:
                    tracker.start()
                    if prof:
                        prof.enable()

                    start_time = time.perf_counter()
                    total_bytes_read = 0
                    read_count = 0

                    for _ in range(iterations):
                        f.seek(0)
                        while True:
                            data = f.read(chunk_bytes)
                            if not data:
                                break
                            total_bytes_read += len(data)
                            read_count += 1

                    elapsed = time.perf_counter() - start_time
                    if prof:
                        prof.disable()
                    tracker.stop()

                    bytes_per_sec = total_bytes_read / max(1e-6, elapsed)
                    results[(cfg_label, chunk_mb)] = bytes_per_sec
                    speed_str = format_speed(bytes_per_sec)

                    peak_mem = tracker.peak_mem_mb
                    peak_cpu = tracker.max_cpu_percent
                    avg_cpu = tracker.avg_cpu_percent

                    print(
                        f"    Chunk Size: {chunk_mb:>3} MB | "
                        f"Total Read Ops: {read_count:>7} | "
                        f"Time: {elapsed:>6.3f}s | "
                        f"Throughput: {speed_str:>10} | "
                        f"Peak Mem: {peak_mem:>7.1f} MB | "
                        f"Peak CPU: {peak_cpu:>5.1f}% (Avg: {avg_cpu:>5.1f}%)",
                        flush=True,
                    )

                    if prof:
                        print(f"\n    --- Top 15 Functions by Cumulative Time (Chunk Size: {chunk_mb} MB) ---", flush=True)
                        stats = pstats.Stats(prof)
                        stats.strip_dirs().sort_stats("cumtime").print_stats(15)
                        print("-" * 80, flush=True)

    # Summary Comparison Table
    print("\n" + "=" * 115, flush=True)
    print(" SUMMARY THROUGHPUT COMPARISON ", flush=True)
    print("=" * 115, flush=True)
    header = f"{'Chunk Size':<12} | {'Direct Buffering':<20} | {'BackgroundPrefetcher':<22} | {'ZeroCopySlabPrefetcher':<24} | {'Winner & Speedup':<25}"
    print(header)
    print("-" * 115)

    for chunk_mb in chunk_sizes_mb:
        direct_speed = results.get(("Direct Buffering (No Prefetch)", chunk_mb), 0.0)
        bg_speed = results.get(("Current BackgroundPrefetcher", chunk_mb), 0.0)
        slab_speed = results.get(("ZeroCopySlabPrefetcher (Fixed Slab Pool + Parallel memoryview)", chunk_mb), 0.0)

        direct_str = format_speed(direct_speed) if direct_speed else "N/A"
        bg_str = format_speed(bg_speed) if bg_speed else "N/A"
        slab_str = format_speed(slab_speed) if slab_speed else "N/A"

        if slab_speed > 0 and bg_speed > 0:
            if slab_speed >= bg_speed:
                pct = ((slab_speed - bg_speed) / bg_speed) * 100.0
                winner_str = f"ZeroCopySlab (+{pct:.1f}%)"
            else:
                pct = ((bg_speed - slab_speed) / slab_speed) * 100.0
                winner_str = f"Background (+{pct:.1f}%)"
        else:
            winner_str = "N/A"

        row = f"{str(chunk_mb) + ' MB':<12} | {direct_str:<20} | {bg_str:<22} | {slab_str:<24} | {winner_str:<25}"
        print(row)

    print("=" * 115 + "\n", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark single-threaded read path throughput using dummy IO with simulated network latency & bandwidth."
    )
    parser.add_argument(
        "--iterations",
        "-i",
        type=int,
        default=1,
        help="Number of iterations to read the complete file per configuration (default: 1)",
    )
    parser.add_argument(
        "--size-gb",
        type=float,
        default=2.0,
        help="Virtual file size in GB (default: 2.0)",
    )
    parser.add_argument(
        "--chunk-sizes",
        "-c",
        type=str,
        default="1,4,5,8,16,64",
        help="Comma-separated chunk sizes in MB (default: '1,4,5,8,16,64')",
    )
    parser.add_argument(
        "--ttfb-ms",
        type=float,
        default=0.0,
        help="Simulated fixed TTFB latency per HTTP range fetch in milliseconds (default: 0.0, e.g. 10.0)",
    )
    parser.add_argument(
        "--bandwidth-mbps",
        type=float,
        default=0.0,
        help="Simulated network bandwidth in MB/s (default: 0.0 for unlimited, e.g. 500.0, 1000.0)",
    )
    parser.add_argument(
        "--byte-latency-ns",
        type=float,
        default=0.0,
        help="Simulated per-byte latency in nanoseconds (default: 0.0, e.g. 1.0 ns/byte = 1000 MB/s)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable detailed cProfile function profiling breakdown",
    )
    args = parser.parse_args()
    chunks = [int(x.strip()) for x in args.chunk_sizes.split(",") if x.strip()]

    run_benchmark(
        iterations=args.iterations,
        file_size_gb=args.size_gb,
        chunk_sizes_mb=chunks,
        ttfb_ms=args.ttfb_ms,
        bandwidth_mbps=args.bandwidth_mbps,
        byte_latency_ns=args.byte_latency_ns,
        profile_enabled=args.profile,
    )
