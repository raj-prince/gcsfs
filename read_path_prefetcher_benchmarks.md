# GCSFS Read Path & Adaptive Prefetcher Optimization Experiment Log

This document tracks experimental single-threaded read path throughput and resource consumption using **Dummy IO** (`dummy_io=True` / `GCSFS_DUMMY_IO=1`) to eliminate remote network latency and isolate Python/GCSFS user-space CPU performance.

---

## Benchmark Methodology & Setup

- **Tooling:** [`data/benchmark_dummy_io.py`](file:///usr/local/google/home/princer/code/gcsfs/data/benchmark_dummy_io.py)
- **Virtual Object Size:** 2.0 GB sequential read stream
- **Test Duration:** 1.5 seconds per configuration
- **Tracked Metrics:**
  - **Throughput:** Processed bytes per second (MB/s and GB/s)
  - **Peak Memory RSS:** Resident Set Size memory peak (MB)
  - **Peak & Avg CPU Utilization:** Single-process CPU core usage (%)

---

## Experiment 0: Initial Baseline (Pre-Optimization)

**Date:** August 12, 2026  
**Status:** Baseline Completed  
**Summary:** Evaluated baseline single-threaded CPU read throughput across chunk sizes (`1 MB`, `5 MB`, `16 MB`, `64 MB`) comparing **Direct Buffering (`cache_type="none"`)** against **Adaptive Prefetching (`use_experimental_adaptive_prefetching=True`)**.

### Table 1: Standard GCSFile (Regional Bucket Read Path)

| Configuration | Chunk Size | Reads | Processed | Throughput (MB/s) | Throughput (GB/s) | Peak Mem (MB) | Peak CPU % | Avg CPU % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct Buffering (No Prefetch)** | 1 MB | 14,695 | 14.35 GB | 9,796.34 MB/s | 9.57 GB/s | 734.7 MB | 129.1% | 107.0% |
| **Direct Buffering (No Prefetch)** | **5 MB** | **7,945** | **38.76 GB** | **26,455.63 MB/s** | **25.84 GB/s** | **734.7 MB** | **131.5%** | **104.4%** |
| **Direct Buffering (No Prefetch)** | 16 MB | 1,960 | 30.62 GB | 20,900.13 MB/s | 20.41 GB/s | 734.7 MB | 344.3% | 101.5% |
| **Direct Buffering (No Prefetch)** | 64 MB | 241 | 15.06 GB | 10,268.89 MB/s | 10.03 GB/s | 734.7 MB | 167.4% | 101.0% |
| **Adaptive Prefetching ON** | 1 MB | 2,899 | 2.83 GB | 1,932.56 MB/s | 1.89 GB/s | 734.7 MB | 129.5% | 100.0% |
| **Adaptive Prefetching ON** | 5 MB | 1,672 | 8.16 GB | 5,567.41 MB/s | 5.44 GB/s | 734.7 MB | 1113.4% | 101.3% |
| **Adaptive Prefetching ON** | **16 MB** | **445** | **6.95 GB** | **4,735.51 MB/s** | **4.62 GB/s** | **734.7 MB** | **123.6%** | **101.0%** |
| **Adaptive Prefetching ON** | 64 MB | 110 | 6.88 GB | 4,669.72 MB/s | 4.56 GB/s | 734.7 MB | 229.4% | 100.8% |

> **Baseline Observation (Regional):** Direct buffering peak throughput (**25.84 GB/s** at 5 MB chunks) is **~4.7x faster** than Adaptive Prefetching peak throughput (**4.62 GB/s** at 16 MB chunks).

---

### Table 2: ZonalFile (High-Performance Zonal Bucket Read Path)

| Configuration | Chunk Size | Reads | Processed | Throughput (MB/s) | Throughput (GB/s) | Peak Mem (MB) | Peak CPU % | Avg CPU % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct Buffering (No Prefetch)** | 1 MB | 13,418 | 13.10 GB | 8,945.13 MB/s | 8.74 GB/s | 734.7 MB | 1162.8% | 106.7% |
| **Direct Buffering (No Prefetch)** | **5 MB** | **7,342** | **35.82 GB** | **24,449.40 MB/s** | **23.88 GB/s** | **734.7 MB** | **128.0%** | **104.6%** |
| **Direct Buffering (No Prefetch)** | 16 MB | 1,947 | 30.42 GB | 20,759.02 MB/s | 20.27 GB/s | 734.7 MB | 115.7% | 101.6% |
| **Direct Buffering (No Prefetch)** | 64 MB | 241 | 15.06 GB | 10,276.09 MB/s | 10.04 GB/s | 734.7 MB | 144.6% | 100.9% |
| **Adaptive Prefetching ON** | 1 MB | 3,099 | 3.03 GB | 2,064.15 MB/s | 2.02 GB/s | 734.7 MB | 130.2% | 100.2% |
| **Adaptive Prefetching ON** | 5 MB | 2,489 | 12.14 GB | 8,282.35 MB/s | 8.09 GB/s | 734.7 MB | 270.5% | 103.7% |
| **Adaptive Prefetching ON** | **16 MB** | **1,107** | **17.30 GB** | **11,803.37 MB/s** | **11.53 GB/s** | **734.7 MB** | **298.6%** | **102.1%** |
| **Adaptive Prefetching ON** | 64 MB | 239 | 14.94 GB | 10,172.88 MB/s | 9.93 GB/s | 734.7 MB | 171.7% | 101.3% |

> **Baseline Observation (Zonal):** Direct buffering peak throughput (**23.88 GB/s** at 5 MB chunks) is **~2.1x faster** than Adaptive Prefetching peak throughput (**11.53 GB/s** at 16 MB chunks).

---

## Identified Prefetcher Bottlenecks ([`gcsfs/prefetcher.py`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py))

Code review of [`gcsfs/prefetcher.py`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py) revealed four architectural CPU bottlenecks causing the prefetcher overhead:

1. **`asyncio.to_thread` Dispatch on Every Read ([`prefetcher.py:L570`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py#L570) & [`L609`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py#L609)):**
   - On every `consume()` call, `PrefetchConsumer` offloads byte slicing (`_fast_slice`) and block assembly (`b"".join`) to Python's global `ThreadPoolExecutor` via `asyncio.to_thread`. Submitting microscopic tasks across thread pool worker queues adds severe lock contention and queueing latency.
2. **Cross-Thread Synchronization on Every Chunk ([`BackgroundPrefetcher.fetch`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py#L864)):**
   - Synchronous read calls `f.read()` invoke `fsspec.asyn.sync(self.loop, self.afetch, start, end)`, hopping across calling and IO worker thread boundaries on every block read.
3. **Producer-Consumer Ping-Pong Event Delays ([`asyncio.Event` + `asyncio.Queue`](file:///usr/local/google/home/princer/code/gcsfs/gcsfs/prefetcher.py#L511)):**
   - The consumer signals `self.wakeup_event.set()` on block consumption, forcing cooperative task yields in `asyncio` for producer recalculations and task queueing.
4. **Cold-Start Streak Multiplier Delay (`MIN_STREAKS_FOR_PREFETCHING = 3`):**
   - Prefetch ahead multiplier remains `1` until after 3 consecutive sequential reads.

---

## Planned Experiments Log

- [x] **Experiment 0 (Baseline):** Profile unmodified prefetcher against direct buffering.
- [ ] **Experiment 1:** Inline small/medium slice operations in `PrefetchConsumer._advance` to eliminate `asyncio.to_thread(_fast_slice)` thread-pool dispatch overhead.
- [ ] **Experiment 2:** Inline `b"".join` inside event loop in `PrefetchConsumer.consume` for single/small chunk lists.
- [ ] **Experiment 3:** Watermark-based producer buffering to eliminate block-by-block `wakeup_event.set()` ping-ponging.
