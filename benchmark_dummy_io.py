"""
Single-Threaded Read Path Dummy IO Benchmark Script with Detailed Function Profiling (cProfile).

Usage:
    python benchmark_dummy_io.py [--iterations N] [--size-gb GB] [--profile]
"""

import argparse
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


def run_benchmark(iterations: int = 1, file_size_gb: float = 5.0, profile_enabled: bool = False):
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

    print("=" * 115, flush=True)
    print(" GCSFS SINGLE-THREADED READ PATH THROUGHPUT BENCHMARK & CPU PROFILER ", flush=True)
    print("=" * 115, flush=True)
    print(f" Virtual File Size: {file_size_gb:.1f} GB | Iterations per case: {iterations} | Profile: {profile_enabled}", flush=True)
    print("=" * 115, flush=True)

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

                    throughput_gb_s = ((total_bytes_read / (1024 * 1024)) / elapsed) / 1024
                    peak_mem = tracker.peak_mem_mb
                    peak_cpu = tracker.max_cpu_percent
                    avg_cpu = tracker.avg_cpu_percent

                    print(
                        f"    Chunk Size: {chunk_mb:>3} MB | "
                        f"Total Read Ops: {read_count:>7} | "
                        f"Throughput: {throughput_gb_s:>6.2f} GB/s | "
                        f"Peak Mem: {peak_mem:>7.1f} MB | "
                        f"Peak CPU: {peak_cpu:>5.1f}% (Avg: {avg_cpu:>5.1f}%)",
                        flush=True,
                    )

                    if prof:
                        print(f"\n    --- Top 15 Functions by Cumulative Time (Chunk Size: {chunk_mb} MB) ---", flush=True)
                        stats = pstats.Stats(prof)
                        stats.strip_dirs().sort_stats("cumtime").print_stats(15)
                        print("-" * 80, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark single-threaded read path throughput using dummy IO with CPU profiling."
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
        "--profile",
        action="store_true",
        help="Enable detailed cProfile function profiling breakdown",
    )
    args = parser.parse_args()
    run_benchmark(iterations=args.iterations, file_size_gb=args.size_gb, profile_enabled=args.profile)
