"""
Real Network IO Benchmark Script for GCSFS Single-Threaded Read Path.

Compares:
1. Direct Buffering (No Prefetch)
2. Current BackgroundPrefetcher
3. ZeroCopySlabPrefetcher (Fixed Slab Pool + Zero-Copy Slicing)

Usage:
    python benchmark_network_io.py --path gs://my-bucket/large-file.bin [--iterations N] [--chunk-sizes 1,4,8,16,64] [--profile]
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
from gcsfs.extended_gcsfs import GCSFileSystem

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


def run_network_benchmark(
    gcs_path: str,
    chunk_sizes_mb: list[int],
    iterations: int = 1,
    profile_enabled: bool = False,
    project: str | None = None,
    token: str | None = None,
):
    # Normalize path
    clean_path = gcs_path
    if clean_path.startswith("gs://"):
        clean_path = clean_path[5:]

    fs = GCSFileSystem(project=project, token=token)

    try:
        file_info = fs.info(clean_path)
        file_size_bytes = file_info["size"]
    except Exception as e:
        print(f"Error accessing path gs://{clean_path}: {e}", file=sys.stderr)
        sys.exit(1)

    file_size_mb = file_size_bytes / (1024 * 1024)
    file_size_gb = file_size_mb / 1024

    configs = [
        ("Direct Buffering (No Prefetch)", False, False, "none"),
        ("Current BackgroundPrefetcher", True, False, "none"),
        ("ZeroCopySlabPrefetcher (Fixed Slab Pool + Zero-Copy)", True, True, "none"),
    ]

    tracker = ResourceTracker(interval=0.03)

    print("=" * 120, flush=True)
    print(" GCSFS REAL NETWORK READ PATH THROUGHPUT BENCHMARK & PROFILER ", flush=True)
    print("=" * 120, flush=True)
    print(f" Target Object: gs://{clean_path}", flush=True)
    print(f" Remote File Size: {file_size_mb:.2f} MB ({file_size_gb:.2f} GB) | Iterations: {iterations} | Profile: {profile_enabled}", flush=True)
    print("=" * 120, flush=True)

    # Store results for final summary table: (config_label, chunk_mb) -> throughput_bytes_per_sec
    results = {}

    for cfg_label, use_prefetch, use_slab, prefetch_cache_type in configs:
        print(f"\n  [ Configuration: {cfg_label} ]", flush=True)
        print("-" * 120, flush=True)

        for chunk_mb in chunk_sizes_mb:
            chunk_bytes = chunk_mb * 1024 * 1024

            open_kwargs = {
                "block_size": chunk_bytes,
                "slab_size": max(1024 * 1024, chunk_bytes),
                "cache_type": prefetch_cache_type,
                "use_experimental_adaptive_prefetching": use_prefetch,
                "use_slab_prefetcher": use_slab,
            }

            prof = cProfile.Profile() if profile_enabled else None

            try:
                with fs.open(clean_path, "rb", **open_kwargs) as f:
                    tracker.start()
                    if prof:
                        prof.enable()

                    start_time = time.perf_counter()
                    total_bytes_read = 0
                    read_count = 0
                    first_byte_time = None

                    for _ in range(iterations):
                        f.seek(0)
                        while True:
                            t_before_chunk = time.perf_counter()
                            data = f.read(chunk_bytes)
                            if not data:
                                break
                            if first_byte_time is None:
                                first_byte_time = time.perf_counter() - t_before_chunk

                            total_bytes_read += len(data)
                            read_count += 1

                    elapsed = time.perf_counter() - start_time
                    if prof:
                        prof.disable()
                    tracker.stop()

                    bytes_per_sec = total_bytes_read / max(1e-6, elapsed)
                    results[(cfg_label, chunk_mb)] = bytes_per_sec

                    speed_str = format_speed(bytes_per_sec)
                    ttfb_str = f"{first_byte_time * 1000:.1f} ms" if first_byte_time else "N/A"
                    peak_mem = tracker.peak_mem_mb
                    peak_cpu = tracker.max_cpu_percent
                    avg_cpu = tracker.avg_cpu_percent

                    print(
                        f"    Chunk: {chunk_mb:>3} MB | "
                        f"Total Read Ops: {read_count:>6} | "
                        f"Time: {elapsed:>6.3f}s | "
                        f"TTFB: {ttfb_str:>8} | "
                        f"Throughput: {speed_str:>10} | "
                        f"Peak RAM: {peak_mem:>7.1f} MB | "
                        f"Peak CPU: {peak_cpu:>5.1f}%",
                        flush=True,
                    )

                    if prof:
                        print(f"\n    --- Top 15 Functions by Cumulative Time (Chunk: {chunk_mb} MB) ---", flush=True)
                        stats = pstats.Stats(prof)
                        stats.strip_dirs().sort_stats("cumtime").print_stats(15)
                        print("-" * 80, flush=True)

            except Exception as e:
                print(f"    Chunk: {chunk_mb:>3} MB | Error during read: {e}", flush=True)

    # Print Comparative Summary Table
    print("\n" + "=" * 120, flush=True)
    print(" SUMMARY THROUGHPUT COMPARISON ", flush=True)
    print("=" * 120, flush=True)
    header = f"{'Chunk Size':<12} | {'Direct Buffering':<20} | {'BackgroundPrefetcher':<22} | {'ZeroCopySlabPrefetcher':<24} | {'Winner & Speedup':<25}"
    print(header)
    print("-" * 120)

    for chunk_mb in chunk_sizes_mb:
        direct_speed = results.get(("Direct Buffering (No Prefetch)", chunk_mb), 0.0)
        bg_speed = results.get(("Current BackgroundPrefetcher", chunk_mb), 0.0)
        slab_speed = results.get(("ZeroCopySlabPrefetcher (Fixed Slab Pool + Zero-Copy)", chunk_mb), 0.0)

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

    print("=" * 120 + "\n", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark single-threaded real network read throughput and memory usage in GCSFS."
    )
    parser.add_argument(
        "--path",
        "-p",
        type=str,
        required=True,
        help="GCS URI/path to real object (e.g. 'gs://bucket-name/file.bin' or 'bucket-name/file.bin').",
    )
    parser.add_argument(
        "--chunk-sizes",
        "-c",
        type=str,
        default="1,4,5,8,16,64",
        help="Comma-separated chunk sizes in MB (default: '1,4,5,8,16,64').",
    )
    parser.add_argument(
        "--iterations",
        "-i",
        type=int,
        default=1,
        help="Number of iterations to read the complete file (default: 1).",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="Google Cloud project ID (optional).",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="GCS credentials token / path (optional, defaults to ambient ADC).",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable cProfile and output top 15 cumulative time CPU hotspots.",
    )

    args = parser.parse_args()
    chunks = [int(x.strip()) for x in args.chunk_sizes.split(",") if x.strip()]

    run_network_benchmark(
        gcs_path=args.path,
        chunk_sizes_mb=chunks,
        iterations=args.iterations,
        profile_enabled=args.profile,
        project=args.project,
        token=args.token,
    )
