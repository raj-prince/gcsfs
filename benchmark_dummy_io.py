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

# By default, mute noisy library logs unless --debug is specified
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
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
    buffer_sizes_mb: list[int] | None = None,
    io_sizes_kb: list[int] | None = None,
    ttfb_ms: float = 0.0,
    bandwidth_mbps: float = 0.0,
    byte_latency_ns: float = 0.0,
    concurrency: int = 1,
    profile_enabled: bool = False,
    engine: str = "all",
    max_prefetch_mb: int = 256,
    bucket_type: str = "all",
    read_method: str = "both",
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

    # Determine test pairs of (buffer_size_bytes, io_size_bytes)
    test_pairs = []
    if buffer_sizes_mb is not None and io_sizes_kb is not None:
        for b_mb in buffer_sizes_mb:
            for io_kb in io_sizes_kb:
                test_pairs.append((b_mb * 1024 * 1024, io_kb * 1024, f"Buffer {b_mb}MB / IO {io_kb}KB", (b_mb, io_kb)))
    elif io_sizes_kb is not None:
        default_block_mb = 5
        for io_kb in io_sizes_kb:
            test_pairs.append((default_block_mb * 1024 * 1024, io_kb * 1024, f"Default 5MB / IO {io_kb}KB", (default_block_mb, io_kb)))
    elif buffer_sizes_mb is not None:
        for b_mb in buffer_sizes_mb:
            test_pairs.append((b_mb * 1024 * 1024, b_mb * 1024 * 1024, f"{b_mb} MB", (b_mb, b_mb * 1024)))
    elif chunk_sizes_mb is not None:
        for c_mb in chunk_sizes_mb:
            test_pairs.append((c_mb * 1024 * 1024, c_mb * 1024 * 1024, f"{c_mb} MB", (c_mb, c_mb * 1024)))
    else:
        for c_mb in [1, 4, 5, 8, 16, 64]:
            test_pairs.append((c_mb * 1024 * 1024, c_mb * 1024 * 1024, f"{c_mb} MB", (c_mb, c_mb * 1024)))

    all_modes = [
        ("Standard GCSFile (Regional)", BucketType.NON_HIERARCHICAL),
        ("Rapid / Zonal (ZonalFile)", BucketType.ZONAL_HIERARCHICAL),
    ]
    if bucket_type in ("rapid", "zonal", "zonal_hierarchical"):
        modes = [all_modes[1]]
    elif bucket_type in ("standard", "regional", "non_hierarchical"):
        modes = [all_modes[0]]
    else:
        modes = all_modes

    all_configs = [
        ("Current BackgroundPrefetcher", True, False, "none"),
        ("ZeroCopySlabPrefetcher (Fixed Slab Pool + Parallel memoryview)", True, True, "none"),
    ]
    if engine in ("slab", "zero_slab"):
        configs = [all_configs[1]]
    elif engine in ("background", "bg"):
        configs = [all_configs[0]]
    else:
        configs = all_configs

    if read_method == "all":
        methods = ["read", "readinto_legacy", "readinto"]
    elif read_method in ("both", "compare"):
        methods = ["read", "readinto_legacy", "readinto"]
    elif read_method in ("read", "readinto", "readinto_legacy"):
        methods = [read_method]
    else:
        raise ValueError(f"Unknown read_method: {read_method}")

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
    print(f" Virtual File Size: {file_size_gb:.1f} GB | Max Prefetch Window: {max_prefetch_mb} MB | Concurrency: {concurrency} | Iterations: {iterations} | Simulation: {', '.join(latency_desc)}", flush=True)
    print("=" * 115, flush=True)

    results = {}

    for mode_name, bucket_type in modes:
        fs._sync_lookup_bucket_type = lambda bucket, _bt=bucket_type: _bt

        print(f"\n>>> Mode: {mode_name}", flush=True)
        print("-" * 115, flush=True)

        for cfg_label, use_prefetch, use_slab, prefetch_cache_type in configs:
            print(f"\n  [ Configuration: {cfg_label} ]", flush=True)

            for method in methods:
                if method == "read":
                    method_label = "read()"
                elif method == "readinto_legacy":
                    method_label = "readinto() [Legacy fsspec fallback: read() + memcpy]"
                elif method == "readinto":
                    method_label = "readinto() [Optimized Inherent fetch_into()]"
                else:
                    method_label = f"{method}()"
                print(f"\n    -- Read Method: {method_label} --", flush=True)

                for buf_bytes, io_bytes, pair_label, pair_key in test_pairs:
                    max_prefetch_bytes = max_prefetch_mb * 1024 * 1024
                    slab_size = max(16 * 1024 * 1024, buf_bytes)
                    num_slabs = max(2, max_prefetch_bytes // slab_size)

                    open_kwargs = {
                        "block_size": buf_bytes,
                        "slab_size": slab_size,
                        "num_slabs": num_slabs,
                        "max_prefetch_bytes": max_prefetch_bytes,
                        "max_prefetch_size": max_prefetch_bytes,
                        "cache_type": prefetch_cache_type,
                        "use_experimental_adaptive_prefetching": use_prefetch,
                        "use_slab_prefetcher": use_slab,
                        "concurrency": concurrency,
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

                        if method == "read":
                            for _ in range(iterations):
                                f.seek(0)
                                while True:
                                    data = f.read(io_bytes)
                                    if not data:
                                        break
                                    total_bytes_read += len(data)
                                    read_count += 1
                        elif method == "readinto_legacy":
                            # Emulate upstream fsspec AbstractBufferedFile.readinto
                            buf = bytearray(io_bytes)
                            out = memoryview(buf).cast("B")
                            for _ in range(iterations):
                                f.seek(0)
                                while True:
                                    data = f.read(len(out))
                                    if not data:
                                        break
                                    out[: len(data)] = data
                                    total_bytes_read += len(data)
                                    read_count += 1
                        elif method == "readinto":
                            # Inherent native readinto with buffer reuse
                            buf = bytearray(io_bytes)
                            for _ in range(iterations):
                                f.seek(0)
                                while True:
                                    n = f.readinto(buf)
                                    if not n:
                                        break
                                    total_bytes_read += n
                                    read_count += 1

                        elapsed = time.perf_counter() - start_time
                        if prof:
                            prof.disable()
                        tracker.stop()

                        bytes_per_sec = total_bytes_read / max(1e-6, elapsed)
                        results[(mode_name, cfg_label, method, pair_key)] = bytes_per_sec
                        results[(cfg_label, method, pair_key)] = bytes_per_sec
                        speed_str = format_speed(bytes_per_sec)

                        peak_mem = tracker.peak_mem_mb
                        peak_cpu = tracker.max_cpu_percent
                        avg_cpu = tracker.avg_cpu_percent

                        print(
                            f"      {pair_label:<24} | "
                            f"Total Read Ops: {read_count:>7} | "
                            f"Time: {elapsed:>6.3f}s | "
                            f"Throughput: {speed_str:>10} | "
                            f"Peak Mem: {peak_mem:>7.1f} MB | "
                            f"Peak CPU: {peak_cpu:>5.1f}% (Avg: {avg_cpu:>5.1f}%)",
                            flush=True,
                        )

                        if prof:
                            print(f"\n      --- Top 15 Functions by Cumulative Time ({method}(), {pair_label}) ---", flush=True)
                            stats = pstats.Stats(prof)
                            stats.strip_dirs().sort_stats("cumtime").print_stats(15)
                            print("-" * 80, flush=True)

    # Summary: READ vs READINTO Comparison Table
    if len(methods) > 1:
        print("\n" + "=" * 135, flush=True)
        print(" READ vs READINTO (LEGACY vs OPTIMIZED) THROUGHPUT COMPARISON ", flush=True)
        print("=" * 135, flush=True)

        for mode_name, _ in modes:
            print(f"\n  [ Mode: {mode_name} ]", flush=True)
            if io_sizes_kb is not None:
                header = f"  {'Configuration':<26} | {'Buffer Size':<15} | {'IO Size':<9} | {'read()':<14} | {'readinto (Legacy)':<19} | {'readinto (Native)':<19} | {'Native vs Legacy':<18} | {'Native vs read':<15}"
                print(header)
                print("  " + "-" * 148)

                b_mb_list = buffer_sizes_mb if buffer_sizes_mb is not None else [5]
                for cfg_label, _, _, _ in configs:
                    for b_mb in b_mb_list:
                        for io_kb in io_sizes_kb:
                            pair_key = (b_mb, io_kb)
                            read_speed = results.get((mode_name, cfg_label, "read", pair_key), 0.0)
                            legacy_speed = results.get((mode_name, cfg_label, "readinto_legacy", pair_key), 0.0)
                            native_speed = results.get((mode_name, cfg_label, "readinto", pair_key), 0.0)

                            read_str = format_speed(read_speed) if read_speed else "N/A"
                            legacy_str = format_speed(legacy_speed) if legacy_speed else "N/A"
                            native_str = format_speed(native_speed) if native_speed else "N/A"

                            if native_speed > 0 and legacy_speed > 0:
                                diff_leg = ((native_speed - legacy_speed) / legacy_speed) * 100.0
                                leg_delta_str = f"{diff_leg:+.1f}%"
                            else:
                                leg_delta_str = "N/A"

                            if native_speed > 0 and read_speed > 0:
                                diff_read = ((native_speed - read_speed) / read_speed) * 100.0
                                read_delta_str = f"{diff_read:+.1f}%"
                            else:
                                read_delta_str = "N/A"

                            buf_str = f"{b_mb} MB" if buffer_sizes_mb is not None else f"Default ({b_mb}MB)"
                            io_str = f"{io_kb} KB" if io_kb < 1024 else f"{io_kb // 1024} MB"
                            row = f"  {cfg_label[:26]:<26} | {buf_str:<15} | {io_str:<9} | {read_str:<14} | {legacy_str:<19} | {native_str:<19} | {leg_delta_str:<18} | {read_delta_str:<15}"
                            print(row)
            else:
                header = f"  {'Configuration':<26} | {'Chunk':<8} | {'read()':<14} | {'readinto (Legacy)':<19} | {'readinto (Native)':<19} | {'Native vs Legacy':<18} | {'Native vs read':<15}"
                print(header)
                print("  " + "-" * 128)

                for cfg_label, _, _, _ in configs:
                    for _, _, pair_label, pair_key in test_pairs:
                        read_speed = results.get((mode_name, cfg_label, "read", pair_key), 0.0)
                        legacy_speed = results.get((mode_name, cfg_label, "readinto_legacy", pair_key), 0.0)
                        native_speed = results.get((mode_name, cfg_label, "readinto", pair_key), 0.0)

                        read_str = format_speed(read_speed) if read_speed else "N/A"
                        legacy_str = format_speed(legacy_speed) if legacy_speed else "N/A"
                        native_str = format_speed(native_speed) if native_speed else "N/A"

                        if native_speed > 0 and legacy_speed > 0:
                            diff_leg = ((native_speed - legacy_speed) / legacy_speed) * 100.0
                            leg_delta_str = f"{diff_leg:+.1f}%"
                        else:
                            leg_delta_str = "N/A"

                        if native_speed > 0 and read_speed > 0:
                            diff_read = ((native_speed - read_speed) / read_speed) * 100.0
                            read_delta_str = f"{diff_read:+.1f}%"
                        else:
                            read_delta_str = "N/A"

                        row = f"  {cfg_label[:26]:<26} | {pair_label:<8} | {read_str:<14} | {legacy_str:<19} | {native_str:<19} | {leg_delta_str:<18} | {read_delta_str:<15}"
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
        default="",
        help="Comma-separated chunk sizes in MB (e.g. '1,4,5,8,16,64')",
    )
    parser.add_argument(
        "--buffer-sizes",
        type=str,
        default="",
        help="Comma-separated buffer / block sizes in MB (e.g. '1,2,4,8')",
    )
    parser.add_argument(
        "--io-sizes-kb",
        type=str,
        default="",
        help="Comma-separated IO sizes in KB (e.g. '256,512,1024,2048')",
    )
    parser.add_argument(
        "--io-sizes-mb",
        type=str,
        default="",
        help="Comma-separated IO sizes in MB (e.g. '0.25,0.5,1,2')",
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
        "--concurrency",
        type=int,
        default=1,
        help="Sub-range download concurrency per chunk / slab (default: 1)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable detailed cProfile function profiling breakdown",
    )
    parser.add_argument(
        "--engine",
        type=str,
        default="all",
        choices=["all", "slab", "zero_slab", "background", "bg"],
        help="Which prefetch engine to run ('all', 'slab', or 'background')",
    )
    parser.add_argument(
        "--read-method",
        type=str,
        default="both",
        choices=["both", "all", "read", "readinto"],
        help="Which read method to benchmark: 'read' (allocate new bytes), 'readinto' (re-use buffer), or 'both' (default: 'both')",
    )
    parser.add_argument(
        "--bucket-type",
        "--mode",
        type=str,
        default="all",
        choices=["all", "rapid", "zonal", "standard", "regional"],
        help="Which bucket type to benchmark ('rapid', 'standard', or 'all')",
    )
    parser.add_argument(
        "--max-prefetch-mb",
        type=int,
        default=256,
        help="Maximum prefetch window size in MB (default: 256, e.g. 512, 1024)",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Enable detailed DEBUG logging for prefetcher and gcsfs",
    )
    args = parser.parse_args()
    if args.debug:
        logging.getLogger("gcsfs").setLevel(logging.DEBUG)
        logging.getLogger("gcsfs.prefetcher").setLevel(logging.DEBUG)
        logging.getLogger("gcsfs.slab_prefetcher").setLevel(logging.DEBUG)

    chunks = [int(x.strip()) for x in args.chunk_sizes.split(",") if x.strip()] if args.chunk_sizes else None
    buf_sizes = [int(x.strip()) for x in args.buffer_sizes.split(",") if x.strip()] if args.buffer_sizes else None
    
    io_sizes = None
    if args.io_sizes_kb:
        io_sizes = [int(x.strip()) for x in args.io_sizes_kb.split(",") if x.strip()]
    elif args.io_sizes_mb:
        io_sizes = [int(float(x.strip()) * 1024) for x in args.io_sizes_mb.split(",") if x.strip()]

    run_benchmark(
        iterations=args.iterations,
        file_size_gb=args.size_gb,
        chunk_sizes_mb=chunks,
        buffer_sizes_mb=buf_sizes,
        io_sizes_kb=io_sizes,
        ttfb_ms=args.ttfb_ms,
        bandwidth_mbps=args.bandwidth_mbps,
        byte_latency_ns=args.byte_latency_ns,
        concurrency=args.concurrency,
        profile_enabled=args.profile,
        engine=args.engine,
        max_prefetch_mb=args.max_prefetch_mb,
        bucket_type=args.bucket_type,
        read_method=args.read_method,
    )
