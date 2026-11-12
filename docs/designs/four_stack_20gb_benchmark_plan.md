# Four-Stack Python & Rust GCS Benchmark Suite (`US-WEST4`, `Transport::Http`)

*   **Author:** Prince Kumar (@princer)
*   **Status:** Approved
*   **Date:** 2026-11-06

--------------------------------------------------------------------------------

## Overview

This document specifies the design, integration changes, and benchmark harness for evaluating four Python client architectures in `gcsfs` against the Native Rust baseline (`gcs-rust-fs` v0.1.1, documented in [`native_rust_fs_benchmark_report.md`](file:///home/princer_google_com/dev/gcs-rust-fs/docs/native_rust_fs_benchmark_report.md)) over the HTTP/JSON data path (`Transport::Http`) on `gs://princer-flat/bench/bench_20GB.bin` in `us-west4`:

1.  **Scenario 1 — Pure Python `gcsfs` Baseline (`Stack 2`)**: Standard `gcsfs.GCSFileSystem` backed by `aiohttp` HTTP/JSON requests and `fsspec` asynchronous event loops (`use_rust_fs=False`).
2.  **Scenario 2 — `gcsfs` Direct PyO3 Binding over `CachedFs<GcsFs>` (`Stack 3`)**: `gcsfs` delegating through the `gcs_rust_fs_py` PyO3 extension linked directly against `gcs_rust_fs::CachedFs<GcsFs>` (`Transport::Http`, with Mode 1 drop-behind sliding-window readahead—which discards pages immediately after consumer read with no persistent page cache—active by default).
3.  **Scenario 3 — `gcsfs` with Python `grpc.aio` Client over Rust UDS Sidecar (`Stack 4a`)**: `gcsfs` using a pure-Python `grpc.aio` client over a Unix Domain Socket (`unix:///tmp/gcsfs-sidecar.sock`) connected to an out-of-process Rust `gcsfs-sidecar` daemon.
4.  **Scenario 4 — `gcsfs` with PyO3 Binding over Rust `RemoteFs` Client over Rust UDS Sidecar (`Stack 4b`)**: `gcsfs` delegating through `gcs_rust_fs_py` wrapping `gcs_rust_fs::RemoteFs` (pooled UDS HTTP/2 channels) over a Unix Domain Socket connected to the out-of-process Rust `gcsfs-sidecar` daemon.

The remainder of this document covers the problem statement and goals, critical user journeys, the four-stack technical architecture, rejected alternatives, non-functional requirements, and the file-by-file implementation and verification plan.

## The Problem

Python data-loading workloads on Google Cloud Storage (`gcsfs`) face severe single-process throughput limits (`~600–800 MB/s`) due to Python GIL contention, single-threaded `asyncio` loop serialization (`fsspecIO`), and memory copies across `aiohttp` and `AbstractBufferedFile`. While `gcs-rust-fs` v0.1.1 achieves `4,106.42 MB/s` (`1T`) and `20,976.12 MB/s` (`48T`) in Native Rust on `gs://princer-flat/bench/bench_20GB.bin` (`us-west4`), the existing `gcs_rust_fs_py` PoC in `gcsfs` cannot evaluate or reproduce these gains across the candidate integration stacks for four reasons:

1.  `rust/gcs_rust_fs_py` depends on `gcs-rust-fs = "0.1.0"` and instantiates bare `GcsFs` without wrapping it in `CachedFs` (Mode 1 drop-behind readahead) or exposing `seek()` on `RustGcsFile`.
2.  Even when `use_rust_fs=True`, `ExtendedGcsFileSystem` and `GCSFile` still initialize and hop onto `fsspec`'s `fsspecIO` loop thread (via `weakref.finalize`, `_sync_lookup_bucket_layout`, `GCSFile.close()`, and `_write_file_cache_update`), and `AbstractBufferedFile` intercepts `f.read()` with either Python `BackgroundPrefetcher` tasks or stateless `_fetch_range` calls that bypass stateful Rust readahead.
3.  `gcsfs` lacks both the pure-Python `grpc.aio` UDS client (`Stack 4a`) and the PyO3 `RemoteFs` UDS client binding (`Stack 4b`) needed to compare in-process PyO3 against out-of-process UDS sidecar architectures.
4.  Running a `48 × 20 GiB = 960 GiB` sequential scan in `48T` (single OS process, 48 Python threads) on GIL-bound stacks (`Pure Python gcsfs` and `Python grpc.aio`) would take ~23 minutes per test without a configurable test duration cap.

## Goals

*   Link `rust/gcs_rust_fs_py` against `/home/princer_google_com/dev/gcs-rust-fs` (`v0.1.1`), wrap `GcsFs` in `CachedFs` (Mode 1 drop-behind readahead) by default, bypass `DirCache` when `cache_timeout == 0` so uncached `info()` tests measure live GCS RPCs, and expose `seek()`, `tell()`, `size()`, and `info()` on `RustGcsFile`.
*   Eliminate `fsspec`'s `asyncio` loop thread (`fsspecIO` / `asyn.sync`) and `AbstractBufferedFile` buffering when `use_rust_fs=True` or `use_uds_grpc=True` by delegating `read()`, `seek()`, `tell()`, `write()`, `flush()`, and `close()` directly to a single unified handle on `GCSFile` (`self._rust_file`) and removing obsolete `_rust_reader` / `_rust_writer` PoC branches.
*   Implement both UDS sidecar client stacks: a stream-bound Python `grpc.aio` UDS backend (`gcsfs/uds_grpc_backend.py`) driven by a per-PID background `asyncio` loop thread (`Stack 4a`), and `gcs_rust_fs::RemoteFs` inside `gcs_rust_fs_py` (`Stack 4b`).
*   Execute the four-part `20 GiB` benchmark suite in `US-WEST4` comparing all four Python scenarios against Stack 1 (Native Rust) on `gs://princer-flat/bench/bench_20GB.bin`:
    1.  **Part 1**: Single-worker uncached `info()` latency (`30` iterations: `mean`, `min`, `p50`, `p90`, `p99`).
    2.  **Part 2**: Single-worker `1 MiB` random read (`50` non-overlapping `4 MiB`-aligned offsets via `open` + `seek` + `read(1 MiB)`: `p50`, `p90`, `p99`, `IOPS`, `MB/s`, `MiB/s`).
    3.  **Part 3**: `20 GiB` sequential read (`8 MiB` chunks) at `1` worker (`20 GiB`), `24` workers (`480 GiB` across both `24T` threads and `24P` processes), and `48` workers (`960 GiB` across both `48T` threads and `48P` processes), with a `120s` (2-minute) duration cap per test (`--max-duration-secs 120`).
    4.  **Part 4**: Single-worker `200 MiB` write (`5` iterations comparing streaming `open("wb")` + `write(8 MiB)` + `close()` against one-shot `pipe_file(200 MiB)`).

## Non-Goals

*   **Multi-Worker Random Read Benchmarks**: Running `1 MiB` random reads at `24` or `48` workers is out of scope; Part 2 runs at `1` worker (`50` reads) matching [`native_rust_fs_benchmark_report.md`](file:///home/princer_google_com/dev/gcs-rust-fs/docs/native_rust_fs_benchmark_report.md).
*   **GCS DirectPath / gRPC (`Transport::Grpc`) Data Reads**: Evaluating `BidiReadObject` against GCS is out of scope; all GCS object data reads and writes use `Transport::Http`.
*   **Zonal (`Rapid`) and Multi-Region Buckets**: Benchmarks target only the co-located regional bucket `gs://princer-flat` in `US-WEST4`.

## Open Questions

*   None; all scope, caching, UDS stream reuse, and concurrency parameters were resolved during the brainstorm and review phases.

## Critical User Journeys (CUJs)

### CUJ 1: Uncached Metadata `info()` Latency Across All Four Python Stacks

*   **Given**: `gs://princer-flat/bench/bench_20GB.bin` in `US-WEST4` and initialized `GCSFileSystem` instances for Pure Python `gcsfs`, Direct PyO3 (`CachedFs<GcsFs>`), Python `grpc.aio` UDS, and PyO3 `RemoteFs` UDS with metadata caching disabled (`cache_timeout=0`, and `gcsfs-sidecar` started without `--cache` or `--readahead` during Part 1 so it wraps bare `GcsFs` with no `DirCache`).
*   **When**: The benchmark runner executes `30` sequential `fs.info(path)` calls on a single worker.
*   **Then**: Every call performs a live GCS metadata lookup without local `DirCache` hits, recording `mean`, `min`, `p50`, `p90`, and `p99` latencies in milliseconds.

### CUJ 2: Single-Worker `1 MiB` Random Read (`open` + `seek` + `read`)

*   **Given**: `50` deterministic, non-overlapping `4 MiB`-aligned byte offsets across `gs://princer-flat/bench/bench_20GB.bin` (`20 GiB`).
*   **When**: A single worker opens the file (`fs.open(path, "rb")`, with `cache_type="none"` in Scenario 1 so Pure Python `gcsfs` does not amplify `1 MiB` reads to `5 MiB` blocks) and executes `f.seek(offset)` followed by `f.read(1048576)` for each of the 50 offsets.
*   **Then**: Each read fetches the exact requested `1 MiB` slice—using a new stream per random read on UDS and `CachedFs` Mode 1 Rule 7 single-read isolation (which fetches only the exact `[offset, offset + len)` range without readahead amplification when a post-`seek()` read is smaller than `io_size`)—and reports `p50`, `p90`, `p99` latency, `IOPS`, and `MB/s` (`MiB/s`).

### CUJ 3: Full-File `20 GiB` Sequential Read Across `1`, `24T`/`24P`, and `48T`/`48P` Workers (Time-Capped at `120s`)

*   **Given**: `gs://princer-flat/bench/bench_20GB.bin` (for `1` worker) and `gs://princer-flat/bench/bench_20GB_00.bin` .. `bench_20GB_47.bin` (for `24` and `48` workers), with Mode 1 drop-behind readahead active in Rust/sidecar (`--readahead`) and `cache_type="readahead", block_size=8*1024*1024` active in Pure Python `gcsfs`.
*   **When**: Workers (spawned via `threading.Thread` for `24T`/`48T` or `multiprocessing.get_context("spawn")` for `24P`/`48P`) open their assigned shard files before a synchronization barrier, then stream `8 MiB` sequential chunks after barrier release until either `20 GiB` per worker is consumed or `120` seconds elapse.
*   **Then**: The runner records per-chunk latencies and total bytes transferred across all workers, reporting wall time, total GiB transferred, chunk `p50` and `p90`, and aggregate `MB/s` (`MiB/s`) for both multi-threaded (`24T`, `48T`) and multi-process (`24P`, `48P`) runs.

### CUJ 4: Single-Worker `200 MiB` Streaming Write and One-Shot `pipe_file`

*   **Given**: A `200 MiB` (`209,715,200` bytes) in-memory payload and target object `gs://princer-flat/bench/bench_20GB_write_200MB.bin`.
*   **When**: A single worker runs `5` iterations of streaming write (`fs.open(path, "wb")` + twenty-five `f.write(8 MiB)` calls + `f.close()`) and `5` iterations of one-shot `fs.pipe_file(path, payload)`.
*   **Then**: The runner outputs `mean`, `p50`, `p90` duration (in seconds) and throughput (`MB/s` and `MiB/s` at `p50`) for all four Python stacks alongside Stack 1 Native Rust.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. System Topology Across the Four Python Scenarios and Native Rust Baseline

All four Python scenarios and the Stack 1 Native Rust baseline communicate with `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`) over the **HTTP/JSON data path (`Transport::Http`)**:

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                         TARGET: gs://princer-flat/bench/bench_20GB.bin                           │
│                  (20.00 GiB Regional Bucket, US-WEST4, HTTP/JSON Data Path)                      │
└──────────▲─────────────────────────▲─────────────────────────────▲───────────────────────────────┘
           │ HTTP Range GET          │ HTTP Range GET              │ HTTP Range GET
┌──────────┴───────────┐  ┌──────────┴─────────────┐  ┌────────────┴───────────────────────────────┐
│ BASELINE (Stack 1):  │  │ SCENARIO 1 (Stack 2):  │  │ SCENARIO 2 (Stack 3):                      │
│ Native Rust          │  │ Pure Python gcsfs      │  │ gcsfs + Direct PyO3 (`CachedFs<GcsFs>`)    │
│ `bench_live_readahead│  │ `use_rust_fs=False`    │  │ `use_rust_fs=True, rust_transport="http"`  │
│ ├── CachedFs Mode 1  │  │ ├── fsspecIO asyncio   │  │ ├── Zero fsspecIO loop (direct py.detach)  │
│ ├── Transport::Http  │  │ ├── aiohttp HTTP/JSON  │  │ ├── GCSFile(cache_type="none") -> _rust_file
│ └── 1T / 24T / 48T   │  │ └── Readahead / None   │  │ └── CachedFs<GcsFs> Mode 1 (48 HTTP pool)  │
└──────────────────────┘  └────────────────────────┘  └────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ UDS SIDECAR SCENARIOS (Scenarios 3 & 4 -> Shared Out-of-Process Rust `gcsfs-sidecar`)            │
│                                                                                                  │
│  ┌─────────────────────────────────────────────┐  ┌───────────────────────────────────────────┐  │
│  │ SCENARIO 3 (Stack 4a):                      │  │ SCENARIO 4 (Stack 4b):                    │  │
│  │ Python `grpc.aio` UDS Client                │  │ PyO3 `RemoteFs` UDS Client                │  │
│  │ `use_uds_grpc=True, uds_socket_path=...`    │  │ `use_rust_fs=True, rust_uds_path=...`     │  │
│  │ ├── Per-PID background asyncio loop thread  │  │ ├── Zero fsspecIO loop (direct py.detach) │  │
│  │ └── UdsGrpcFile (stream reuse + frame buf)  │  │ └── RustGcsFile -> RemoteFs (16 UDS conns)│  │
│  └──────────────────────┬──────────────────────┘  └─────────────────────┬─────────────────────┘  │
│                         │ unix:///tmp/gcsfs-sidecar.sock                │                        │
│                         ▼                                               ▼                        │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ Out-of-Process Rust Daemon: `gcsfs-sidecar --transport http [--readahead] --http-pool-size 48`│
│  │ ├── Part 1 (info): runs without `--readahead`/`--cache` (bare GcsFs, zero DirCache)        │  │
│  │ ├── Parts 2–4 (I/O): runs with `--readahead` (CachedFs Mode 1, ReadPattern::Auto)          │  │
│  │ └── Striped Transport::Http Client Pool (48 Storage clients) ──► GCS US-WEST4 (HTTP/JSON)  │  │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2. Scenario 2 & Scenario 4: `gcs_rust_fs_py` (`v0.1.1`) & Zero-`fsspecIO` Direct Path

#### A. `CachedFs<GcsFs>` and `RemoteFs` Backend Dispatch in [`rust/gcs_rust_fs_py/src/lib.rs`](file:///home/princer_google_com/c1dev/gcsfs/rust/gcs_rust_fs_py/src/lib.rs)

*   **Crate Upgrade**: [`rust/gcs_rust_fs_py/Cargo.toml`](file:///home/princer_google_com/c1dev/gcsfs/rust/gcs_rust_fs_py/Cargo.toml) links against `gcs-rust-fs = { version = "0.1.1", path = "/home/princer_google_com/dev/gcs-rust-fs" }`.
*   **Backend Representation**:
    *   **Direct In-Process Mode (Scenario 2)**: When no UDS path is configured, `gcs_rust_fs_py` builds `GcsFs` over `Transport::Http` with a configurable HTTP client pool (`48` clients by default or via `GCS_RUST_FS_HTTP_POOL_SIZE`) and wraps it in `CachedFs` with Mode 1 drop-behind readahead (`256 KiB` pages, `16 MiB` I/O folios, up to `256 MiB` readahead window, `24` max inflight tasks). When uncached metadata lookup is requested (`fs.cache_timeout == 0`), `info()` calls the inner `GcsFs` directly to bypass `DirCache` while keeping Mode 1 readahead active for file reads.
    *   **Remote UDS Mode (Scenario 4)**: When `rust_uds_path` is provided, `gcs_rust_fs_py` connects to `gcsfs-sidecar` via `RemoteFs` with a pool of `16` UDS HTTP/2 channels.
*   **Stateful File Handle (`RustGcsFile`)**:
    *   Wraps the trait object `Box<dyn gcs_rust_fs::File>`, caches file size, cursor position, and open-time object metadata (normalized to match `GCSFileSystem.info` keys so opening a file in read mode requires only a single RPC), and executes `read`, `seek`, `write`, `flush`, and `close` on the Tokio runtime with the Python GIL released.

#### B. Eliminating `fsspecIO` Event Loop and `AbstractBufferedFile` Buffering in [`gcsfs/core.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/core.py) & [`gcsfs/extended_gcsfs.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/extended_gcsfs.py)

When `use_rust_fs=True` (Scenarios 2 and 4) or `use_uds_grpc=True` (Scenario 3):
1.  **Zero `fsspecIO` Loop Initialization or Bucket Layout Hop**:
    *   `ExtendedGcsFileSystem.__init__` passes `self._loop` (not the lazy `self.loop` property) to its finalizer so constructing a filesystem never starts the `fsspecIO` thread.
    *   `ExtendedGcsFileSystem._open` skips `_sync_lookup_bucket_layout` and instantiates `GCSFile` directly.
    *   `GCSFileSystem.info`, `cat_file`, and `pipe_file` route directly to the synchronous Rust or UDS backend (even when `self.dircache` is populated, invalidating the cache synchronously after `pipe_file`).
2.  **Single Unified Delegate Handle (`self._rust_file`) and Lifecycle Short-Circuiting on `GCSFile`**:
    *   Because `RustGcsFile` and `UdsGrpcFile` implement the same synchronous file handle interface, `GCSFile` binds a single delegate attribute (`self._rust_file`) and removes the legacy `_rust_reader` and `_rust_writer` PoC branches.
    *   In read mode (`"rb"`), `GCSFile.__init__` opens `self._rust_file` before `super().__init__()` to pass the known object size and restores `self._details` immediately after `super().__init__()` so `AbstractBufferedFile` never triggers a second `fs.info()` RPC. In write mode (`"wb"` / `"xb"`), `GCSFile.__init__` opens `self._rust_file` after `content_type`, `metadata`, and `blocksize` are initialized.
    *   `read`, `seek`, `tell`, and `write` delegate directly to `self._rust_file`. `flush`, `_close_impl`, `close`, and `discard` short-circuit `AbstractBufferedFile`'s upload state machine and `self.gcsfs.loop` checks, closing `self._rust_file` and invalidating the parent directory cache synchronously on write completion.

### 3. Scenario 3: Pure-Python `grpc.aio` UDS Sidecar Client ([`gcsfs/uds_grpc_backend.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/uds_grpc_backend.py))

*   **Generated Protobuf Stubs (`gcsfs/_proto/`)**:
    *   Compile [`/home/princer_google_com/dev/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto`](file:///home/princer_google_com/dev/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto) via `grpc_tools.protoc` into `gcsfs/_proto/filesystem_pb2.py` and `gcsfs/_proto/filesystem_pb2_grpc.py`.
*   **Per-PID Background `asyncio` Loop & Channel Pool**:
    *   Because `grpc.aio` channels and open streaming RPCs are bound to the event loop on which they were created and must be safely callable from `1`, `24T`, or `48T` worker threads as well as `24P`/`48P` worker processes, `uds_grpc_backend.py` runs a dedicated per-PID background `asyncio` daemon thread owning a pool of `grpc.aio` UDS channels (`16 MiB` max message size, `4 MiB` HTTP/2 frame size). Synchronous methods schedule coroutines onto that background loop thread.
*   **Stream-Bound `UdsGrpcFile`**:
    *   **Sequential Read (`f.read(8 MiB)`)**: Opens a server-streaming `FileSystemService.Read` call on the generation-pinned object path (`gs://bucket/key#generation` when generation is known, or `gs://bucket/key`) from the current offset to EOF on first read, buffers unread tail bytes across `8 MiB` `ReadResponse` messages, and reuses the open stream across subsequent `f.read()` calls until EOF or `seek()`.
    *   **Random Read (`f.seek(offset)` + `f.read(1 MiB)`)**: `seek(offset)` closes any active stream, clears the frame buffer, and marks the handle as seeked. The next `read(length)` performs an isolated range stream for `[offset, offset + length)` (triggering the sidecar's `<= 4 MiB` single-read fast path without readahead amplification) and clears the seeked flag so any subsequent consecutive reads resume streaming.
    *   **Writes (`open("wb")` + `write(8 MiB)` + `close()` and `pipe_file`)**: Streams `WriteSpec` followed by `8 MiB` data frames to `FileSystemService.Write`.

### 4. Four-Part Benchmark Execution & Concurrency Specification

All four Python scenarios are benchmarked by [`benchmarks/bench_four_stacks_20gb.py`](file:///home/princer_google_com/c1dev/gcsfs/benchmarks/bench_four_stacks_20gb.py) against `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`):

| Part | Workload | Concurrency | Details & Metrics |
| :--- | :--- | :--- | :--- |
| **Part 1** | Uncached `info()` Latency | `1` worker | `30` uncached `fs.info(path)` calls (`cache_timeout=0`; sidecar started **without** `--readahead` or `--cache` so `DirCache` is absent). Reports `mean`, `min`, `p50`, `p90`, `p99` (ms). |
| **Part 2** | `1 MiB` Random Read | `1` worker | `50` non-overlapping `4 MiB`-aligned offsets (`splitmix64` seed matching Rust Stack 1) via `open("rb", cache_type="none")` + `seek(off)` + `read(1 MiB)`. Sidecar runs with `--readahead`. Reports `p50`, `p90`, `p99` (ms), `IOPS`, `MB/s`, `MiB/s`. |
| **Part 3** | `20 GiB` Sequential Read (`8 MiB` chunks) | `1`, `24T`, `24P`, `48T`, `48P` | Streams `bench_20GB.bin` (`1` worker) or `bench_20GB_00.bin..47.bin` (`24` / `48` workers, using `multiprocessing.get_context("spawn")` for `24P`/`48P` and `AdaptiveReadaheadCache` with `block_size=8 MiB` for Scenario 1) after a post-`open()` readiness barrier, capped at `--max-duration-secs 120` per test. In Scenarios 3 and 4, all `24T`/`48T` threads and all `24P`/`48P` spawned worker processes connect concurrently to a **single shared `gcsfs-sidecar` UDS server process** (`/tmp/gcsfs-sidecar-bench.sock`). Reports wall time, total GiB read, chunk `p50` & `p90` (ms), `MB/s`, `MiB/s`. |
| **Part 4** | `200 MiB` Write | `1` worker | `5` iterations of streaming `open("wb")` + `25 × write(8 MiB)` + `close()` and `5` iterations of one-shot `pipe_file(200 MiB)`. Reports `mean`, `p50`, `p90` (s), and `MB/s` (`MiB/s`) at `p50`. |

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Keep `fsspec` `AbstractBufferedFile` (`cache_type="readahead"` / `"adaptive"`) in Front of `RustGcsFile` and `UdsGrpcFile`

*   **Pros**: Requires no changes to `GCSFile.read()` or `GCSFile.seek()` in `gcsfs/core.py`.
*   **Cons**: `AbstractBufferedFile` intercepts `f.read()` and either spawns a Python `BackgroundPrefetcher` on the `fsspecIO` loop thread (which calls `_cat_file_concurrent` and bypasses `RustGcsFile` completely) or issues stateless `_fetch_range(start, end)` (`read_range`) calls, bypassing `CachedFs` Mode 1 `ReadaheadFile` and adding redundant Python memory copies.
*   **Reason for Rejection**: Rejected by author: `GCSFile` must set `cache_type="none"` and delegate `read()`, `seek()`, and `tell()` directly to `RustGcsFile` / `UdsGrpcFile` without `fsspecIO` event-loop hops or double-buffering.

### Alternative B: Benchmark Only Python `grpc.aio` or Only PyO3 `RemoteFs` for the UDS Sidecar Stack

*   **Pros**: Reduces the benchmark matrix from four Python scenarios to three.
*   **Cons**: Benchmarking only Python `grpc.aio` over UDS conflates Python `asyncio` / protobuf frame handling overhead with the Unix Domain Socket kernel IPC overhead; benchmarking only PyO3 `RemoteFs` leaves out how a pure-Python gRPC UDS client performs.
*   **Reason for Rejection**: Rejected by author: Evaluating both Scenario 3 (Python `grpc.aio` over UDS) and Scenario 4 (PyO3 `RemoteFs` over UDS) against the same `gcsfs-sidecar` daemon isolates Python `grpc.aio` client overhead from the UDS IPC hop itself.

### Alternative C: Benchmark High-Concurrency Sequential Reads Only with Multi-Processing (`24P` / `48P`) or Only with Multi-Threading (`24T` / `48T`)

*   **Pros**: Cuts the number of Part 3 concurrency runs in half.
*   **Cons**: Multi-threading (`24T` / `48T` in a single Python process) is where PyO3 (`py.detach`) shines by scaling across Tokio worker threads without multi-process memory duplication, whereas Pure Python `gcsfs` and Python `grpc.aio` serialize behind the Python GIL and single `asyncio` event loop thread unless scaled across OS processes (`24P` / `48P`).
*   **Reason for Rejection**: Rejected by author: Testing both `24T`/`48T` and `24P`/`48P` (with a `120s` per-test duration cap for slow configurations) directly measures both single-process multi-threaded scaling and multi-process scaling across all four stacks.

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Performance & Measurement Integrity
*   **Uncached Metadata Measurement**: Part 1 (`info()` latency) must execute with local metadata caching disabled (`cache_timeout=0`, bypassing `CachedFs` `DirCache` in Scenario 2, and running `gcsfs-sidecar` without `--readahead` or `--cache` in Scenarios 3 & 4 before restarting `gcsfs-sidecar` with `--readahead` for Parts 2–4) so all 30 iterations measure live GCS RPC round-trips.
*   **Fork-Safe Multi-Processing (`spawn`) & Post-`open()` Synchronization Barrier**: All multi-process runs (`24P`, `48P`) must use `multiprocessing.get_context("spawn")` so Tokio and `grpc.aio` runtime threads are initialized cleanly inside each worker process. All multi-worker runs (`24T`, `24P`, `48T`, `48P`) must complete client initialization and `fs.open(shard_path, "rb")` before waiting on a shared readiness barrier (`threading.Barrier` or `multiprocessing.Barrier`) so connection warmup and `open()` metadata lookups do not skew sequential I/O throughput.
*   **Bounded Benchmark Runtime (`--max-duration-secs 120`)**: Each Part 3 sequential read test caps execution at `120` seconds after barrier release, computing throughput from actual bytes read divided by elapsed wall time.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Target Files to Create/Modify

#### 1. [`rust/gcs_rust_fs_py/Cargo.toml`](file:///home/princer_google_com/c1dev/gcsfs/rust/gcs_rust_fs_py/Cargo.toml) (Modify)

*   **Type / Scope**: Rust PyO3 Extension Manifest
*   **Proposed Changes**:
    *   Upgrade `gcs-rust-fs` dependency from `"0.1.0"` to `{ version = "0.1.1", path = "/home/princer_google_com/dev/gcs-rust-fs" }`.
*   **Rationale**: Pulls in `gcs-rust-fs` v0.1.1 with `CachedFs` Mode 1 drop-behind readahead (`DataConfig::readahead_only()`), HTTP connection pool striping (`http_pool_size`), and `RemoteFs` pooled UDS gRPC client support (`connect_uds_with_pool_size`).

#### 2. [`rust/gcs_rust_fs_py/src/lib.rs`](file:///home/princer_google_com/c1dev/gcsfs/rust/gcs_rust_fs_py/src/lib.rs) (Modify)

*   **Type / Scope**: Rust PyO3 Native Extension Module (`gcs_rust_fs_py`)
*   **Proposed Changes**:
    *   Default `GCSFS_RUST_WORKER_THREADS` to `32` (or env override) and replace `FS_CACHE` value type with an enum `RustBackend { Direct(Arc<CachedFs<GcsFs>>), RemoteUds(Arc<RemoteFs>) }` keyed by `(transport, endpoint, uds_path)`.
    *   When `uds_path` is `None`, build `GcsFs` with `http_pool_size` (defaulting to `48` or `GCS_RUST_FS_HTTP_POOL_SIZE`) and wrap it in `CachedFs::new(gcs, cache_cfg)` with Mode 1 `ReadaheadConfig` (`page_size = 256 KiB`, `io_size = 16 MiB` or `GCS_RUST_FS_RA_IO_SIZE_MB`, `max_folio_size = 16 MiB`, `max_readahead = 256 MiB` or `GCS_RUST_FS_RA_MAX_READAHEAD_MB`, `max_inflight = 24` or `GCS_RUST_FS_RA_MAX_INFLIGHT`, `max_inflight_bytes = 4 GiB`).
    *   When `uds_path` is `Some(path)`, connect via `RemoteFs::connect_uds_with_pool_size(path, uds_pool_size)` (defaulting `uds_pool_size` to `16` or `GCS_RUST_FS_UDS_POOL_SIZE`).
    *   Update `info` and `info_async` signatures to accept `uncached: bool = false` and `uds_path: Option<String> = None`; when `uncached` is `true` on `RustBackend::Direct`, call `cached.inner().info(&target).await` directly to bypass `DirCache`.
    *   Update `cat_file`, `cat_file_async`, `pipe_file`, `pipe_file_async`, and `RustGcsFile::open_sync` to accept `uds_path: Option<String> = None`.
    *   Extend `RustGcsFile` (which holds `inner: Arc<AsyncMutex<Box<dyn File>>>`, `pos: AtomicU64`, `size: Option<u64>`, `entry: Option<Entry>`) with:
        *   `seek(py, pos: i64, whence: i32 = 0) -> PyResult<u64>`: maps `whence` (`0 -> SeekFrom::Start`, `1 -> SeekFrom::Current`, `2 -> SeekFrom::End`), delegates to `guard.seek(seek_from)` inside `py.detach(|| get_runtime()?.block_on(...))` so `ReadaheadFile::seek` and `RemoteFile::seek` reset internal readahead/stream state, and updates `self.pos`.
        *   `tell() -> PyResult<u64>`: returns `self.pos.load(Ordering::Relaxed)`.
        *   `size() -> Option<u64>`: returns `self.size`.
        *   `info(py) -> PyResult<Py<PyAny>>`: converts the cached `Entry::file(file.path(), size, file.stat().cloned())` captured at `open_sync` time via `entry_to_py_dict(py, &entry)` without issuing a second RPC.
*   **Rationale**: Enables both Scenario 2 (`CachedFs<GcsFs>`) and Scenario 4 (`RemoteFs` over UDS) behind the same zero-`fsspecIO` PyO3 interface with full `seek()` and uncached `info()` support.

#### 3. [`gcsfs/_proto/__init__.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/_proto/__init__.py), [`gcsfs/_proto/filesystem_pb2.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/_proto/filesystem_pb2.py), & [`gcsfs/_proto/filesystem_pb2_grpc.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/_proto/filesystem_pb2_grpc.py) (Create)

*   **Type / Scope**: Generated Python Protobuf & gRPC Stubs for `gcsfs.v1.FileSystemService`
*   **Proposed Changes**:
    *   Generate Python protobuf message classes and `FileSystemServiceStub` from [`/home/princer_google_com/dev/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto`](file:///home/princer_google_com/dev/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto) using `grpc_tools.protoc` and fix relative import in `filesystem_pb2_grpc.py`.
*   **Rationale**: Required by Scenario 3 (`gcsfs/uds_grpc_backend.py`) to communicate with `gcsfs-sidecar` over Unix Domain Sockets.

#### 4. [`gcsfs/uds_grpc_backend.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/uds_grpc_backend.py) (Create)

*   **Type / Scope**: Pure-Python `grpc.aio` UDS Sidecar Client Adapter (Scenario 3)
*   **Proposed Changes**:
    *   Maintain a per-PID (`os.getpid()`) background `asyncio` daemon event-loop thread and channel pool (`grpc.aio.insecure_channel(f"unix://{uds_path}")`, `16 MiB` max message size, `4 MiB` frame size), dispatching synchronous calls via `asyncio.run_coroutine_threadsafe(coro, loop).result()`.
    *   Implement `_entry_pb_to_info_dict(fs, entry)` (converting protobuf `Entry` / `ObjectStat` fields to `gcsfs` camelCase keys, stringified `generation`/`metageneration`, base64 big-endian `crc32c`, and parsed `mtime`/`ctime` via `_finalize_info_dict`) and `_translate_grpc_error(rpc_error)` (mapping `NOT_FOUND -> FileNotFoundError`, `ALREADY_EXISTS -> FileExistsError`, `INVALID_ARGUMENT` / `OUT_OF_RANGE -> ValueError`, `PERMISSION_DENIED -> PermissionError`).
    *   Implement `uds_info_sync`, `uds_cat_file_sync`, `uds_pipe_file_sync`, and `UdsGrpcFile`:
        *   On `UdsGrpcFile("rb")`, perform an initial `Info` RPC to capture `_details`, `size`, and `pinned_path` (`f"gs://{bucket}/{key}#{generation}"` when `generation > 0`, else `f"gs://{bucket}/{key}"`).
        *   Maintain an open server-streaming `stub.Read` call and internal `bytearray` buffer (`self._stream_buf`) so sequential `read(length)` calls reuse the open stream and slice/reassemble across `8 MiB` `ReadResponse` frames.
        *   On `seek(loc, whence)`, cancel/close any active `Read` stream on the background loop, clear `self._stream_buf`, update `self.loc`, and set `self._seeked = True`. On the next `read(length)`, open a stream for `[self.loc, self.loc + length)` when `self._seeked` is `True` and `length <= 4 MiB`, then reset `self._seeked = False` so subsequent consecutive reads resume full streaming.
        *   On `write(data)` and `close()`, stream `WriteSpec` and `8 MiB`-capped `WriteRequest(data=...)` chunks to `stub.Write`.
*   **Rationale**: Implements Scenario 3 (`gcsfs` with Python `grpc.aio` over Rust UDS sidecar) safely across `1`, `24T`/`48T`, and `24P`/`48P` concurrency modes.

#### 5. [`gcsfs/rust_backend.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/rust_backend.py) (Modify)

*   **Type / Scope**: Python Adapter for `gcs_rust_fs_py` (Scenarios 2 & 4)
*   **Proposed Changes**:
    *   Pass `uncached=(fs.cache_timeout == 0)` and `uds_path=getattr(fs, "rust_uds_path", None)` into `bridge.info` / `bridge.info_async`.
    *   Pass `uds_path=getattr(fs, "rust_uds_path", None)` into `cat_file`, `pipe_file`, and `RustGcsFile.open_sync`.
    *   Expose `rust_file_info(fs, handle)` using `_finalize_info_dict(fs, handle.info())`.
*   **Rationale**: Connects `GCSFileSystem` configuration (`cache_timeout=0`, `rust_uds_path`) to the updated PyO3 functions in `gcs_rust_fs_py`.

#### 6. [`gcsfs/core.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/core.py) & [`gcsfs/extended_gcsfs.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/extended_gcsfs.py) (Modify)

*   **Type / Scope**: Core `GCSFileSystem`, `ExtendedGcsFileSystem`, and `GCSFile` Integration
*   **Proposed Changes**:
    *   In `GCSFileSystem.__init__`: accept `rust_uds_path: str | None = None` (or `GCSFS_RUST_UDS_PATH`) and `use_uds_grpc: bool = False` (or `GCSFS_USE_UDS_GRPC`), `uds_socket_path: str | None = None` (or `GCSFS_UDS_SOCKET_PATH`).
    *   In `ExtendedGcsFileSystem.__init__`: pass `self._loop` instead of `self.loop` to `weakref.finalize` so instantiating the filesystem does not spawn `fsspecIO`.
    *   In `ExtendedGcsFileSystem._open`: when `self.use_rust_fs` or `self.use_uds_grpc` is `True`, skip `self._sync_lookup_bucket_layout(bucket)` and instantiate `GCSFile` directly without touching `fsspecIO`.
    *   In `GCSFileSystem.info`, `cat_file`, and `pipe_file`: route to `rust_backend` when `self.use_rust_fs` is `True` or `uds_grpc_backend` when `self.use_uds_grpc` is `True`, without calling `asyn.sync(self.loop, ...)` (removing the `and not self.dircache` guard on `pipe_file` and calling `self.invalidate_cache(path)` directly).
    *   In `GCSFile.__init__`: when `gcsfs.use_rust_fs` or `gcsfs.use_uds_grpc` is `True`, force `cache_type = "none"`, skip `gcsfs.loop`, and bind a single unified delegate attribute `self._rust_file` (removing legacy `self._rust_reader` and `self._rust_writer` branches):
        *   In `"rb"` mode, open `self._rust_file` before `super().__init__()`, extract `det = rust_file_info(...)` / `uds_file.info()`, pass `size=det["size"]` into `super().__init__()`, and re-assign `self._details = det` immediately after `super().__init__()` so `det = self.details` at line 2851 does not trigger a duplicate `fs.info()` RPC.
        *   In `"wb"` / `"xb"` mode, open `self._rust_file` after `self.content_type`, `self.metadata`, and `self.blocksize` are initialized.
    *   In `GCSFile`: override `read(length=-1)`, `seek(loc, whence=0)`, `tell()`, `write(data)`, `flush(force=False)`, `close()`, `_close_impl()`, and `discard()` to delegate directly to `self._rust_file`, checking `getattr(self.gcsfs, "_loop", None)` in `close()`, setting `self.forced = True` and `self.closed = True` in `_close_impl()`, and calling `self.gcsfs.invalidate_cache(self.path)` synchronously on write close without calling `super().close()` or `asyn.sync(self.gcsfs.loop, ...)`.
*   **Rationale**: Eliminates the `fsspec` event loop (`fsspecIO`) and `AbstractBufferedFile` double-buffering in Scenarios 2, 3, and 4.

#### 7. [`gcsfs/tests/test_gcs_rust_fs_poc.py`](file:///home/princer_google_com/c1dev/gcsfs/gcsfs/tests/test_gcs_rust_fs_poc.py) (Modify)

*   **Type / Scope**: Functional & Integration Tests
*   **Proposed Changes**:
    *   Add unit and integration tests covering `GCSFile.seek()`, `GCSFile.read()` over `CachedFs` Mode 1 readahead, uncached `info(cache_timeout=0)`, Scenario 3 (`use_uds_grpc=True` against `gcsfs-sidecar`), and Scenario 4 (`use_rust_fs=True, rust_uds_path=...` against `gcsfs-sidecar`), plus verifying that `fs._loop` remains `None` (zero `fsspecIO` thread started) across `open`, `read`, `seek`, `write`, `pipe_file`, and `close` when `use_rust_fs=True`.
*   **Rationale**: Validates correctness of all four stacks before running the 20 GiB live benchmark suite.

#### 8. [`benchmarks/bench_four_stacks_20gb.py`](file:///home/princer_google_com/c1dev/gcsfs/benchmarks/bench_four_stacks_20gb.py) & [`docs/four_stack_20gb_benchmark_report.md`](file:///home/princer_google_com/c1dev/gcsfs/docs/four_stack_20gb_benchmark_report.md) (Create)

*   **Type / Scope**: Four-Stack Live `20 GiB` Benchmark Runner & Comparison Report
*   **Proposed Changes**:
    *   Create `benchmarks/bench_four_stacks_20gb.py` to:
        *   Start `gcsfs-sidecar --transport http --http-pool-size 48` (without `--readahead` or `--cache`) for Part 1 (`info()` latency), then restart `gcsfs-sidecar --transport http --readahead --http-pool-size 48` for Parts 2, 3 (`1`, `24T`, `24P`, `48T`, `48P` using `multiprocessing.get_context("spawn")` and `--max-duration-secs 120`), and 4 across Scenarios 1, 2, 3, and 4 on `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`).
    *   Write all measured results and side-by-side comparison tables against Stack 1 Native Rust ([`native_rust_fs_benchmark_report.md`](file:///home/princer_google_com/dev/gcs-rust-fs/docs/native_rust_fs_benchmark_report.md)) to `docs/four_stack_20gb_benchmark_report.md`.
*   **Rationale**: Produces the reproducible benchmark harness and complete comparison report across all stacks and scenarios.

### Verification Plan

*   **Compilation/Build**:
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc cargo build --release --bin gcsfs-sidecar --manifest-path /home/princer_google_com/dev/gcs-rust-fs/Cargo.toml`
    *   `/home/princer_google_com/c1dev/gcsfs/.venv/bin/python -m grpc_tools.protoc -I/home/princer_google_com/dev/gcs-rust-fs/proto --python_out=gcsfs/_proto --grpc_python_out=gcsfs/_proto /home/princer_google_com/dev/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto`
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc /home/princer_google_com/c1dev/gcsfs/.venv/bin/maturin develop --release --manifest-path rust/gcs_rust_fs_py/Cargo.toml`
*   **Unit & Integration Tests**:
    *   `/home/princer_google_com/c1dev/gcsfs/.venv/bin/pytest gcsfs/tests/test_gcs_rust_fs_poc.py -v`
*   **Live 20 GiB Four-Stack Benchmark Suite (`US-WEST4`)**:
    *   `/home/princer_google_com/c1dev/gcsfs/.venv/bin/python benchmarks/bench_four_stacks_20gb.py --path gs://princer-flat/bench/bench_20GB.bin --max-duration-secs 120 --json-out benchmarks/results_four_stacks_20gb.json`

--------------------------------------------------------------------------------

*Drafted with EGM*
