# GCSFS Read Path CPU & Latency Profiling Guide (`py-spy`, `viztracer`, `cProfile`)

This document is a complete technical manual for profiling single-threaded and multi-threaded Python CPU bottlenecks and `asyncio` event-loop scheduling latency in GCSFS using **`py-spy`**, **`viztracer`**, and built-in **`cProfile`**.

---

## 1. Quick Reference: Which Profiler Should You Use?

| Profiler | Mechanism | Runtime Overhead | Best For | Output Format |
| :--- | :--- | :--- | :--- | :--- |
| **`py-spy`** | Out-of-process sampling (Rust) | **< 1%** | Production flamegraphs, CPU sampling, zero-distortion benchmarks | Interactive `.svg` Flamegraph, Live Terminal Top |
| **`viztracer`** | In-process deterministic event tracing | **~5–10%** | `asyncio` sequence diagrams, coroutine wakeups, thread timelines | Chrome / Perfetto `.json` timeline viewer |
| **`cProfile`** | Built-in function call counter | **~15–20%** | Deterministic call counts, single-function execution time tables | `.prof` stats file, terminal tables, `snakeviz` |

---

## 2. Using `py-spy` for Zero-Overhead Flamegraphs

**`py-spy`** samples Python call stacks out-of-process using operating system kernel memory inspection (`process_vm_readv` on Linux). It does not pause your Python GIL or inject bytecode instrumentation, making it the most accurate tool for measuring true benchmark throughput.

### Installation
```bash
.venv/bin/pip install --index-url https://pypi.org/simple py-spy
```

### Command 1: Record an Interactive SVG Flamegraph
Run `py-spy` directly wrapping your script execution:

```bash
# Record a full CPU flamegraph for the single-threaded read benchmark
.venv/bin/py-spy record \
    --format flamegraph \
    --rate 1000 \
    --output gcsfs_flamegraph.svg \
    -- .venv/bin/python benchmark_dummy_io.py --runtime 3.0 --size-gb 5.0
```

#### Key Flags:
- `--format flamegraph`: Generates a standard interactive SVG flamegraph. You can also use `--format speedscope` for [SpeedScope.app](https://www.speedscope.app/) format.
- `--rate 1000`: Sample frequency in Hz (1000 samples/sec per thread gives microsecond-level stack depth resolution).
- `--subprocesses`: Automatically profile child/worker processes if spawned.
- `--idle`: Include threads when they are blocked waiting on system calls or IO (useful for debugging where time is spent asleep).

### Command 2: Watch Live Top Functions in Terminal (`py-spy top`)
To see which functions are consuming CPU in real time while a benchmark is running:

```bash
# Start your script in the background or terminal 1:
.venv/bin/python benchmark_dummy_io.py --runtime 30.0 --size-gb 50.0 &
PID=$!

# Watch top functions live in terminal 2:
.venv/bin/py-spy top --pid $PID
```

---

### How to Inspect the SVG Flamegraph in a Browser
1. Open `gcsfs_flamegraph.svg` in Chrome, Firefox, Safari, or Edge.
2. **Horizontal Width:** Indicates the percentage of total CPU sample time spent inside a function or its children.
3. **Vertical Stack:** Bottom boxes are top-level execution entry points; top boxes are leaf execution frames.
4. **Interactive Controls:**
   - **Click any frame** to zoom in and magnify that specific call sub-tree.
   - **Click "Reset Zoom"** (top right) to return to root view.
   - **Search Box (top right / `Ctrl+F`):** Type symbol names such as `selectors`, `prefetcher`, or `_cat_file` to highlight matching functions in neon yellow across all call paths.

---

## 3. Using `viztracer` for Asyncio Timeline & Coroutine Sequencing

While `py-spy` tells you *which functions execute CPU cycles*, **`viztracer`** records an exact chronologically sequenced **timeline trace** of every thread, `asyncio` task, coroutine context switch, and lock wait event.

### Installation
```bash
.venv/bin/pip install viztracer
```

### Command 1: Capture Asyncio Sequence & Task Trace
```bash
.venv/bin/viztracer \
    --async_profiler \
    --output gcsfs_async_trace.json \
    .venv/bin/python benchmark_dummy_io.py --runtime 2.0 --size-gb 2.0
```

#### Key Flags:
- `--async_profiler`: Automatically tracks `asyncio` coroutine creation, task scheduling, and yield points separately on visual swimlanes.
- `--log_async`: Logs individual `asyncio.Task` names and lifecycle transitions.
- `--max_stack_depth 20`: Limits call stack recording depth to keep JSON trace files lightweight.

---

### How to Visualize `viztracer` Diagrams
You can inspect `gcsfs_async_trace.json` locally or in the web browser:

#### Option A: Built-in Local Viewer
```bash
.venv/bin/vizviewer gcsfs_async_trace.json
```
This automatically launches a local web server and opens the Chrome Trace Viewer UI in your browser (`http://localhost:8086`).

#### Option B: Google Perfetto Web UI (Recommended)
1. Navigate to **[ui.perfetto.dev](https://ui.perfetto.dev/)** in your browser.
2. Drag and drop `gcsfs_async_trace.json` into the page.
3. Use keyboard shortcuts:
   - `W` / `S`: Zoom in / Zoom out timeline
   - `A` / `D`: Pan left / Pan right across wall-clock time
   - `F`: Zoom into currently selected slice

### What to Look for in `viztracer` Swimlanes for GCSFS:
1. **Thread 0 (Main Reader):** Watch where `f.read()` calls wait on `fsspec.asyn.sync()`.
2. **Asyncio Loop Thread:** Watch `PrefetchProducer._process_prefetch_cycle` vs `PrefetchConsumer._advance`.
3. **Gaps between boxes:** Gaps in a swimlane indicate the thread or task yielded CPU control waiting on OS `selectors.select()` or `asyncio.Event`.

---

## 4. Built-in `cProfile` (Zero-Dependency Method)

If you need quick microsecond-level table breakdowns without installing third-party packages, use the `--profile` option built into `benchmark_dummy_io.py`:

```bash
.venv/bin/python benchmark_dummy_io.py --profile --runtime 2.0 --size-gb 2.0
```

### Dump Stats to Interactive Visualizer (`snakeviz`)
You can dump raw `cProfile` binaries to disk and render icicle diagrams:

```python
import cProfile

prof = cProfile.Profile()
prof.enable()
# Perform reads
prof.disable()
prof.dump_stats("read_path.prof")
```

Renders flame/icicle diagrams:
```bash
pip install snakeviz
snakeviz read_path.prof
```
