#!/usr/bin/env python3
"""
Single-Threaded Read Path Dummy IO Benchmark Script for GCSFS with CPU & Memory Resource Tracking.

This script benchmarks the single-threaded CPU processing limit of the GCSFS read path
by eliminating network cost using dummy IO (`dummy_io=True` / `GCSFS_DUMMY_IO=1`), while tracking:
- Maximum throughput (MB/s and GB/s)
- Peak RSS memory usage (MB)
- Peak CPU utilization (%) and Average CPU utilization (%)

Usage:
    python data/benchmark_dummy_io.py [--runtime SECONDS] [--size-gb GB]
"""

import argparse
import logging
import os
import resource
import sys
import threading
import time

# Ensure local repository root is prioritized in sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import gcsfs
from gcsfs.extended_gcsfs import BucketType

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Suppress noise warnings for clean tabular output
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
            self._proc.cpu_percent(interval=None)  # prime cpu_percent counter
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
                    # Linux rusage fallback (in KB)
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

        # Compare OS maxrss fallback
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


def run_benchmark(runtime_sec: float = 3.0, file_size_gb: float = 5.0):
    os.environ["GCSFS_DUMMY_IO"] = "1"

    fs = gcsfs.GCSFileSystem(token="anon", dummy_io=True)

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
    chunk_sizes_mb = [1, 5, 16, 64]

    modes = [
        ("Standard GCSFile (Regional)", BucketType.NON_HIERARCHICAL, "readahead"),
        # ("ZonalFile (High-Perf gRPC Path)", BucketType.ZONAL_HIERARCHICAL, "readahead_chunked"),
    ]

    tracker = ResourceTracker(interval=0.03)

    print("=" * 115, flush=True)
    print(" GCSFS SINGLE-THREADED READ PATH THROUGHPUT BENCHMARK (WITH DUMMY IO & RESOURCE MONITORING) ", flush=True)
    print("=" * 115, flush=True)
    print(f" Virtual File Size: {file_size_gb:.1f} GB", flush=True)
    print(f" Runtime per case: {runtime_sec:.1f} seconds | Tracking: Memory RSS (MB) & CPU Utilization (%)", flush=True)
    print("=" * 115, flush=True)

    for mode_name, bucket_type, prefetch_cache_type in modes:
        fs._sync_lookup_bucket_type = lambda bucket, _bt=bucket_type: _bt

        print(f"\n>>> Mode: {mode_name}", flush=True)
        print("-" * 115, flush=True)

        for use_prefetch in [False, True]:
            prefetch_label = (
                "Adaptive Prefetching ON" if use_prefetch else "Direct Buffering (No Prefetch)"
            )
            print(f"\n  [ Configuration: {prefetch_label} ]", flush=True)

            for chunk_mb in chunk_sizes_mb:
                chunk_bytes = chunk_mb * 1024 * 1024

                open_kwargs = {
                    "block_size": chunk_bytes,
                    "cache_type": prefetch_cache_type if use_prefetch else "none",
                    "use_experimental_adaptive_prefetching": use_prefetch,
                    "dummy_io": True,
                    "size": file_size_bytes,
                }

                with fs.open(path, "rb", **open_kwargs) as f:
                    tracker.start()
                    start_time = time.perf_counter()
                    total_bytes_read = 0
                    read_count = 0

                    while True:
                        now = time.perf_counter()
                        if now - start_time >= runtime_sec:
                            break
                        data = f.read(chunk_bytes)
                        if not data:
                            f.seek(0)
                            continue
                        total_bytes_read += len(data)
                        read_count += 1

                    elapsed = time.perf_counter() - start_time
                    tracker.stop()

                    throughput_mb_s = (total_bytes_read / (1024 * 1024)) / elapsed
                    throughput_gb_s = throughput_mb_s / 1024
                    peak_mem = tracker.peak_mem_mb
                    peak_cpu = tracker.max_cpu_percent
                    avg_cpu = tracker.avg_cpu_percent

                    print(
                        f"    Chunk Size: {chunk_mb:>3} MB | "
                        f"Reads: {read_count:>7} | "
                        f"Throughput: {throughput_mb_s:>9.2f} MB/s ({throughput_gb_s:>5.2f} GB/s) | "
                        f"Peak Mem: {peak_mem:>7.1f} MB | "
                        f"Peak CPU: {peak_cpu:>5.1f}% (Avg: {avg_cpu:>5.1f}%)",
                        flush=True,
                    )
    print("\n" + "=" * 115, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark single-threaded read path throughput using dummy IO with resource monitoring."
    )
    parser.add_argument(
        "--runtime",
        type=float,
        default=3.0,
        help="Duration in seconds to run each benchmark configuration (default: 3.0)",
    )
    parser.add_argument(
        "--size-gb",
        type=float,
        default=5.0,
        help="Virtual file size in GB (default: 5.0)",
    )
    args = parser.parse_args()
    run_benchmark(runtime_sec=args.runtime, file_size_gb=args.size_gb)
