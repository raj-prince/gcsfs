"""Multi-process Logical & Wire I/O Tracer and Interactive HTML Visualizer.

Captures file I/O events across multiple processes (forked or spawned workers)
for fsspec / gcsfs / HuggingFace streaming workloads, recording:
  - PID & Thread ID / Name
  - Layer: "logical" (caller f.read) vs "wire" (_fetch_range / HTTP Range / gRPC)
  - File path & File size
  - Byte offset & Length
  - Start timestamp & End timestamp (seconds relative to trace start)
  - Cache hit indicator (for logical reads)

Generates a standalone interactive HTML5 Canvas dashboard with linked time-zoom.
"""

from __future__ import annotations

import contextlib
import glob
import json
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

_TLS = threading.local()
_hooks_installed = False
_ORIGINAL_METHODS: dict[str, Any] = {}


class _ProcessEventLogger:
    """Fork-safe, lock-free per-PID JSONL event writer."""

    def __init__(self) -> None:
        self._pid: int | None = None
        self._fp: Any = None
        self._trace_dir: str | None = None
        self._t0: float = 0.0
        self._lock = threading.Lock()

    def _ensure_open(self) -> bool:
        trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
        if not trace_dir:
            return False
        pid = os.getpid()
        if self._pid != pid or self._fp is None or self._trace_dir != trace_dir:
            if self._fp is not None:
                with contextlib.suppress(Exception):
                    # Do not flush/close parent's fd buffer in child after fork; just detach
                    if self._pid == pid:
                        self._fp.close()
            self._trace_dir = trace_dir
            self._pid = pid
            self._t0 = float(os.environ.get("GCSFS_IO_TRACE_T0", "0.0") or time.time())
            os.makedirs(trace_dir, exist_ok=True)
            log_path = os.path.join(trace_dir, f"events_{pid}_{id(self)}.jsonl")
            self._fp = open(log_path, "a", buffering=65536, encoding="utf-8")
        return True

    def log(
        self,
        *,
        layer: str,
        op: str,
        file: str,
        offset: int,
        length: int,
        t_start: float,
        t_end: float,
        file_size: int = 0,
        cache_hit: bool | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if not self._ensure_open():
                return
            rec: dict[str, Any] = {
                "pid": self._pid,
                "ppid": os.getppid(),
                "tid": threading.get_ident(),
                "tname": threading.current_thread().name,
                "layer": layer,
                "op": op,
                "file": _normalize_path(file),
                "fsize": int(file_size or 0),
                "off": int(offset or 0),
                "len": int(length or 0),
                "ts": round(t_start - self._t0, 6),
                "te": round(t_end - self._t0, 6),
            }
            if cache_hit is not None:
                rec["hit"] = bool(cache_hit)
            if extra:
                rec.update(extra)
            try:
                self._fp.write(json.dumps(rec, separators=(",", ":")) + "\n")
                # Flush periodically or immediately for visibility across short-lived workers
                self._fp.flush()
            except Exception:
                pass

    def flush(self) -> None:
        with self._lock:
            if self._fp is not None and self._pid == os.getpid():
                with contextlib.suppress(Exception):
                    self._fp.flush()


_LOGGER = _ProcessEventLogger()


def _normalize_path(path: Any) -> str:
    if not isinstance(path, str):
        path = str(path)
    for prefix in ("gs://", "file://", "local://", "bfile://"):
        if path.startswith(prefix):
            path = path[len(prefix) :]
    return path


def _short_filename(path: str) -> str:
    parts = path.rstrip("/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return parts[-1] if parts else path


def install_hooks_in_process() -> None:
    """Install monkey-patches on fsspec AbstractBufferedFile and gcsfs GCSFileSystem."""
    global _hooks_installed
    if _hooks_installed:
        return
    _hooks_installed = True

    import fsspec.spec
    from fsspec.implementations.local import LocalFileOpener, LocalFileSystem
    from fsspec.spec import AbstractBufferedFile

    # 1. Hook AbstractBufferedFile.read (Logical layer)
    orig_abf_read = AbstractBufferedFile.read
    _ORIGINAL_METHODS["abf_read"] = orig_abf_read

    def traced_abf_read(self: Any, length: int = -1) -> bytes:
        if not os.environ.get("GCSFS_IO_TRACE_DIR"):
            return orig_abf_read(self, length)
        loc_before = getattr(self, "loc", 0)
        wire_before = getattr(_TLS, "wire_count", 0)
        t0 = time.time()
        out = orig_abf_read(self, length)
        t1 = time.time()
        wire_after = getattr(_TLS, "wire_count", 0)
        actual_len = len(out) if out is not None else 0
        if actual_len > 0 or length > 0:
            cache_obj = getattr(self, "cache", None)
            cache_name = getattr(cache_obj, "name", type(cache_obj).__name__) if cache_obj else "none"
            _LOGGER.log(
                layer="logical",
                op="read",
                file=getattr(self, "path", "unknown"),
                offset=loc_before,
                length=actual_len,
                t_start=t0,
                t_end=t1,
                file_size=getattr(self, "size", 0) or 0,
                cache_hit=(wire_after == wire_before),
                extra={"req_len": int(length if length is not None else -1), "cache": str(cache_name)},
            )
        return out

    AbstractBufferedFile.read = traced_abf_read

    # 2. Hook AbstractBufferedFile._fetch_range (Wire layer fallback for any fsspec buffered file)
    orig_abf_fetch = AbstractBufferedFile._fetch_range
    _ORIGINAL_METHODS["abf_fetch"] = orig_abf_fetch

    def _wrap_fetch_range(orig_fn: Any) -> Any:
        def traced_fetch_range(self: Any, start: int | None = None, end: int | None = None, *args: Any, **kwargs: Any) -> Any:
            if not os.environ.get("GCSFS_IO_TRACE_DIR"):
                return orig_fn(self, start, end, *args, **kwargs)
            _TLS.wire_count = getattr(_TLS, "wire_count", 0) + 1
            depth = getattr(_TLS, "in_fetch_range", 0)
            _TLS.in_fetch_range = depth + 1
            t0 = time.time()
            try:
                out = orig_fn(self, start, end, *args, **kwargs)
            finally:
                t1 = time.time()
                _TLS.in_fetch_range = depth
            # Only log here if inner _cat_file_sequential didn't already log it
            if depth == 0 and not getattr(_TLS, "logged_by_cat_file", False):
                s = int(start or 0)
                actual_len = len(out) if isinstance(out, (bytes, bytearray, memoryview)) else int((end or s) - s)
                _LOGGER.log(
                    layer="wire",
                    op="fetch_range",
                    file=getattr(self, "path", "unknown"),
                    offset=s,
                    length=actual_len,
                    t_start=t0,
                    t_end=t1,
                    file_size=getattr(self, "size", 0) or 0,
                )
            _TLS.logged_by_cat_file = False
            return out

        return traced_fetch_range

    AbstractBufferedFile._fetch_range = _wrap_fetch_range(orig_abf_fetch)

    # 3. Hook GCSFileSystem & GCSFile if gcsfs is importable
    with contextlib.suppress(ImportError):
        import gcsfs.core

        orig_gcs_fetch = gcsfs.core.GCSFile._fetch_range
        _ORIGINAL_METHODS["gcs_fetch"] = orig_gcs_fetch
        gcsfs.core.GCSFile._fetch_range = _wrap_fetch_range(orig_gcs_fetch)

        orig_cat_seq = gcsfs.core.GCSFileSystem._cat_file_sequential
        _ORIGINAL_METHODS["gcs_cat_seq"] = orig_cat_seq

        async def traced_cat_file_sequential(
            self: Any, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
        ) -> bytes:
            if not os.environ.get("GCSFS_IO_TRACE_DIR"):
                return await orig_cat_seq(self, path, start=start, end=end, **kwargs)
            _TLS.wire_count = getattr(_TLS, "wire_count", 0) + 1
            t0 = time.time()
            out = await orig_cat_seq(self, path, start=start, end=end, **kwargs)
            t1 = time.time()
            _TLS.logged_by_cat_file = True
            s = int(start or 0)
            actual_len = len(out) if out is not None else int((end or s) - s)
            # Try to look up cached file size if known
            fsize = 0
            with contextlib.suppress(Exception):
                if path in self.dircache:
                    fsize = self.dircache[path].get("size", 0)
            _LOGGER.log(
                layer="wire",
                op="http_range_get",
                file=path,
                offset=s,
                length=actual_len,
                t_start=t0,
                t_end=t1,
                file_size=fsize,
            )
            return out

        gcsfs.core.GCSFileSystem._cat_file_sequential = traced_cat_file_sequential

    # 4. Support local file:// simulation of remote buffered reads (so local HF tests show realistic Wire + Logical layers)
    orig_local_open = LocalFileSystem._open
    _ORIGINAL_METHODS["local_open"] = orig_local_open

    class _SimulatedRemoteLocalFile(AbstractBufferedFile):
        def _fetch_range(self, start: int, end: int) -> bytes:
            sim_lat = float(os.environ.get("GCSFS_IO_TRACE_SIM_LATENCY_MS", "0") or 0.0)
            if sim_lat > 0:
                time.sleep(sim_lat / 1000.0)
            with open(self.path, "rb") as f:
                f.seek(start)
                return f.read(max(0, end - start))

    traced_sim_fetch = _wrap_fetch_range(_SimulatedRemoteLocalFile._fetch_range)
    _SimulatedRemoteLocalFile._fetch_range = traced_sim_fetch

    def traced_local_open(
        self: Any,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        **kwargs: Any,
    ) -> Any:
        trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
        sim_block = int(os.environ.get("GCSFS_IO_TRACE_LOCAL_BLOCK_SIZE", "0") or 0)
        stripped = self._strip_protocol(path)
        # Never trace the tracer's own log files or python internals
        if (
            trace_dir
            and sim_block > 0
            and "r" in mode
            and not stripped.startswith(trace_dir)
            and any( stripped.endswith(ext) for ext in (".parquet", ".jsonl", ".arrow", ".bin", ".safetensors", ".pt", ".tar", ".csv") )
        ):
            fsize = self.info(stripped)["size"]
            cache_type = os.environ.get("GCSFS_IO_TRACE_LOCAL_CACHE_TYPE", "readahead")
            return _SimulatedRemoteLocalFile(
                self,
                stripped,
                mode=mode,
                block_size=sim_block,
                size=fsize,
                cache_type=cache_type,
                **kwargs,
            )
        return orig_local_open(self, path, mode=mode, block_size=block_size, **kwargs)

    LocalFileSystem._open = traced_local_open

    # Also hook LocalFileOpener.read when sim_block == 0
    orig_lfo_read = LocalFileOpener.read
    _ORIGINAL_METHODS["lfo_read"] = orig_lfo_read

    def traced_lfo_read(self: Any, length: int = -1) -> bytes:
        trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
        if not trace_dir or str(self.path).startswith(trace_dir):
            return orig_lfo_read(self, length)
        loc_before = self.f.tell()
        t0 = time.time()
        out = orig_lfo_read(self, length)
        t1 = time.time()
        actual_len = len(out) if out is not None else 0
        if actual_len > 0:
            with contextlib.suppress(Exception):
                fsize = os.stat(self.path).st_size
            _LOGGER.log(
                layer="logical",
                op="read",
                file=str(self.path),
                offset=loc_before,
                length=actual_len,
                t_start=t0,
                t_end=t1,
                file_size=fsize,
                cache_hit=False,
            )
        return out

    LocalFileOpener.read = traced_lfo_read

    # 5. Hook HuggingFace datasets.utils.file_utils.xopen so local streaming tests also go through fsspec
    with contextlib.suppress(ImportError):
        import datasets.streaming
        import datasets.utils.file_utils as hf_file_utils

        orig_xopen = hf_file_utils.xopen
        _ORIGINAL_METHODS["hf_xopen"] = orig_xopen

        def traced_xopen(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
            trace_dir = os.environ.get("GCSFS_IO_TRACE_DIR")
            sim_block = int(os.environ.get("GCSFS_IO_TRACE_LOCAL_BLOCK_SIZE", "0") or 0)
            file_str = str(file)
            main_hop = file_str.split("::")[0]
            if (
                trace_dir
                and sim_block > 0
                and "r" in mode
                and hf_file_utils.is_local_path(main_hop)
                and not main_hop.startswith(trace_dir)
            ):
                fs = LocalFileSystem()
                fobj = fs.open(main_hop, mode=mode)
                return hf_file_utils._add_retries_to_file_obj_read_method(fobj)
            return orig_xopen(file, mode, *args, **kwargs)

        hf_file_utils.xopen = traced_xopen
        datasets.streaming.xopen = traced_xopen


@contextlib.contextmanager
def trace_io(
    output_html: str | Path = "io_trace.html",
    *,
    title: str = "Workload I/O Pattern Trace",
    local_block_size: int = 512 * 1024,
    local_cache_type: str = "readahead",
    simulated_wire_latency_ms: float = 1.5,
    keep_raw_events: str | Path | None = None,
):
    """Context manager that traces multi-PID Logical & Wire I/O and compiles an interactive HTML dashboard.

    Parameters
    ----------
    output_html : str or Path
        Destination path for the standalone interactive HTML dashboard.
    title : str
        Title displayed in the dashboard header.
    local_block_size : int
        When reading local files (e.g. local Parquet tests without GCS), wraps local dataset files
        in fsspec's AbstractBufferedFile with this block size so both Wire (_fetch_range) and
        Logical (f.read) layers are captured realistically. Set to 0 to disable.
    local_cache_type : str
        fsspec cache_type ("readahead", "blockcache", "bytes", "none") for local simulation.
    simulated_wire_latency_ms : float
        Simulated network latency per wire block fetch when reading local files (ms).
    keep_raw_events : str or Path, optional
        If provided, saves the merged JSONL events to this path.
    """
    trace_dir = tempfile.mkdtemp(prefix="gcsfs_io_trace_")
    bootstrap_dir = os.path.join(trace_dir, "_bootstrap")
    os.makedirs(bootstrap_dir, exist_ok=True)

    # Write sitecustomize.py so spawned child processes also install the hooks automatically
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sitecustomize_code = f"""import os, sys
if {this_dir!r} not in sys.path:
    sys.path.insert(0, {this_dir!r})
try:
    import io_tracer
    io_tracer.install_hooks_in_process()
except Exception:
    pass
"""
    with open(os.path.join(bootstrap_dir, "sitecustomize.py"), "w", encoding="utf-8") as f:
        f.write(sitecustomize_code)

    old_env = {
        k: os.environ.get(k)
        for k in (
            "GCSFS_IO_TRACE_DIR",
            "GCSFS_IO_TRACE_T0",
            "GCSFS_IO_TRACE_LOCAL_BLOCK_SIZE",
            "GCSFS_IO_TRACE_LOCAL_CACHE_TYPE",
            "GCSFS_IO_TRACE_SIM_LATENCY_MS",
            "PYTHONPATH",
        )
    }

    t0 = time.time()
    os.environ["GCSFS_IO_TRACE_DIR"] = trace_dir
    os.environ["GCSFS_IO_TRACE_T0"] = f"{t0:.6f}"
    os.environ["GCSFS_IO_TRACE_LOCAL_BLOCK_SIZE"] = str(local_block_size)
    os.environ["GCSFS_IO_TRACE_LOCAL_CACHE_TYPE"] = str(local_cache_type)
    os.environ["GCSFS_IO_TRACE_SIM_LATENCY_MS"] = str(simulated_wire_latency_ms)
    existing_pp = old_env.get("PYTHONPATH")
    os.environ["PYTHONPATH"] = f"{bootstrap_dir}:{existing_pp}" if existing_pp else bootstrap_dir

    install_hooks_in_process()

    events: list[dict[str, Any]] = []
    try:
        yield events
    finally:
        _LOGGER.flush()
        # Restore environment
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

        # Collect and merge all per-PID event logs
        for path in sorted(glob.glob(os.path.join(trace_dir, "events_*.jsonl"))):
            with contextlib.suppress(Exception):
                with open(path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if line:
                            events.append(json.loads(line))

        events.sort(key=lambda e: (e.get("ts", 0.0), 0 if e.get("layer") == "wire" else 1))

        if keep_raw_events:
            Path(keep_raw_events).parent.mkdir(parents=True, exist_ok=True)
            with open(keep_raw_events, "w", encoding="utf-8") as fp:
                for ev in events:
                    fp.write(json.dumps(ev) + "\n")

        render_html_dashboard(events, output_html=output_html, title=title, main_pid=os.getpid())
        shutil.rmtree(trace_dir, ignore_errors=True)


def render_html_dashboard(
    events: list[dict[str, Any]],
    *,
    output_html: str | Path,
    title: str = "Workload I/O Pattern Trace",
    main_pid: int | None = None,
) -> Path:
    """Compile trace events into a self-contained interactive HTML5 Canvas dashboard."""
    out_path = Path(output_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Compute file metadata (short names and max observed size)
    file_sizes: dict[str, int] = {}
    for ev in events:
        f = ev.get("file", "unknown")
        end_byte = int(ev.get("off", 0)) + int(ev.get("len", 0))
        fsize = max(int(ev.get("fsize", 0)), end_byte)
        if f not in file_sizes or fsize > file_sizes[f]:
            file_sizes[f] = fsize

    files_meta = [
        {"path": f, "short": _short_filename(f), "size": max(1, sz)}
        for f, sz in sorted(file_sizes.items())
    ]

    # Collect unique PIDs in order of first appearance (with main_pid first if present)
    pids_seen: list[int] = []
    for ev in events:
        p = int(ev.get("pid", 0))
        if p not in pids_seen:
            pids_seen.append(p)
    if main_pid and main_pid in pids_seen:
        pids_seen.remove(main_pid)
        pids_seen.insert(0, main_pid)

    pids_meta = []
    worker_idx = 0
    for p in pids_seen:
        if main_pid is not None and p == main_pid:
            role = "Main Process"
        else:
            role = f"Worker {worker_idx}"
            worker_idx += 1
        pids_meta.append({"pid": p, "role": role})

    payload_json = json.dumps(
        {
            "title": title,
            "main_pid": main_pid,
            "files": files_meta,
            "pids": pids_meta,
            "events": events,
        },
        separators=(",", ":"),
    )

    html = _HTML_TEMPLATE.replace("__TRACE_PAYLOAD_JSON__", payload_json).replace(
        "__TRACE_TITLE__", title
    )
    out_path.write_text(html, encoding="utf-8")
    return out_path


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__TRACE_TITLE__</title>
<style>
  :root {
    --bg: #0b0f19;
    --panel: #111827;
    --panel-alt: #1e293b;
    --border: #334155;
    --text: #f1f5f9;
    --muted: #94a3b8;
    --accent: #38bdf8;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    padding: 16px 20px 32px;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, monospace;
    font-size: 13px;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 14px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
  }
  h1 {
    margin: 0;
    font-size: 18px;
    font-weight: 700;
    letter-spacing: -0.01em;
    color: #fff;
  }
  .subtitle {
    color: var(--muted);
    font-size: 12px;
    margin-top: 3px;
  }
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
    gap: 10px;
    margin-bottom: 14px;
  }
  .kpi-card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 12px;
  }
  .kpi-label {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .kpi-val {
    font-size: 18px;
    font-weight: 700;
    margin-top: 4px;
    color: #fff;
  }
  .kpi-sub {
    color: var(--muted);
    font-size: 11px;
    margin-top: 2px;
  }
  .controls-bar {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 14px;
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    align-items: center;
  }
  .ctrl-group {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .ctrl-label {
    color: var(--muted);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
  }
  .pid-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 999px;
    border: 1px solid var(--border);
    background: var(--panel-alt);
    color: var(--text);
    font-size: 12px;
    cursor: pointer;
    user-select: none;
    transition: all 0.12s;
  }
  .pid-pill.inactive {
    opacity: 0.32;
    filter: grayscale(0.8);
  }
  .pid-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }
  button, select {
    background: var(--panel-alt);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 5px 10px;
    font-size: 12px;
    cursor: pointer;
  }
  button:hover, select:hover {
    border-color: var(--accent);
  }
  label.chk {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    cursor: pointer;
    user-select: none;
    font-size: 12px;
  }
  .legend-box {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 3px 8px;
    border-radius: 5px;
    background: rgba(255,255,255,0.03);
  }
  .swatch-wire {
    width: 14px;
    height: 12px;
    background: rgba(56, 189, 248, 0.25);
    border: 1.5px dashed #38bdf8;
    border-radius: 2px;
  }
  .swatch-logical {
    width: 14px;
    height: 12px;
    background: #38bdf8;
    border: 1px solid #fff;
    border-radius: 2px;
  }
  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    margin-bottom: 14px;
    overflow: hidden;
  }
  .panel-header {
    padding: 8px 14px;
    background: rgba(255,255,255,0.02);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .panel-title {
    font-weight: 600;
    font-size: 13px;
    color: #e2e8f0;
  }
  .panel-hint {
    color: var(--muted);
    font-size: 11px;
  }
  .canvas-wrap {
    position: relative;
    width: 100%;
  }
  canvas {
    display: block;
    width: 100%;
    cursor: crosshair;
  }
  #tooltip {
    position: fixed;
    pointer-events: none;
    display: none;
    z-index: 9999;
    background: rgba(15, 23, 42, 0.96);
    border: 1px solid #475569;
    border-radius: 8px;
    padding: 10px 12px;
    box-shadow: 0 10px 25px rgba(0,0,0,0.65);
    font-size: 12px;
    min-width: 260px;
    max-width: 380px;
    line-height: 1.45;
  }
  .tt-head {
    font-weight: 700;
    font-size: 13px;
    margin-bottom: 6px;
    padding-bottom: 4px;
    border-bottom: 1px solid #334155;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .tt-badge {
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    text-transform: uppercase;
    font-weight: 700;
  }
  .tt-row {
    display: flex;
    justify-content: space-between;
    gap: 12px;
  }
  .tt-k { color: var(--muted); }
  .tt-v { font-weight: 600; color: #f8fafc; text-align: right; word-break: break-all; }
  .tables-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));
    gap: 14px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  th, td {
    padding: 7px 10px;
    text-align: left;
    border-bottom: 1px solid #1e293b;
  }
  th {
    color: var(--muted);
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    background: rgba(255,255,255,0.02);
  }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
  .amp-good { color: #4ade80; font-weight: 600; }
  .amp-warn { color: #facc15; font-weight: 600; }
  .amp-bad  { color: #f87171; font-weight: 600; }
</style>
</head>
<body>

<header>
  <div>
    <h1 id="hdr-title">Workload I/O Pattern Explorer</h1>
    <div class="subtitle">Multi-PID Space-Time File Offset, Length &amp; Layer (Logical vs. Wire) Visualizer</div>
  </div>
  <div class="ctrl-group">
    <div class="legend-box"><span class="swatch-wire"></span> Wire Fetch (Network / Block Range)</div>
    <div class="legend-box"><span class="swatch-logical"></span> Logical Read (Caller / f.read)</div>
    <button id="btn-reset-zoom">Reset Zoom (Full Trace)</button>
  </div>
</header>

<div class="kpi-grid" id="kpi-grid"></div>

<div class="controls-bar">
  <div class="ctrl-group">
    <span class="ctrl-label">PIDs (Color):</span>
    <div id="pid-pills" class="ctrl-group"></div>
    <button id="btn-all-pids">All PIDs</button>
  </div>

  <div class="ctrl-group">
    <span class="ctrl-label">Layers:</span>
    <label class="chk"><input type="checkbox" id="chk-wire" checked> Wire Fetches</label>
    <label class="chk"><input type="checkbox" id="chk-logical" checked> Logical Reads</label>
    <label class="chk"><input type="checkbox" id="chk-misses-only"> Logical Cache Misses Only</label>
  </div>

  <div class="ctrl-group">
    <span class="ctrl-label">File / Y-Axis View:</span>
    <select id="sel-file-mode">
      <option value="swimlanes">File Swimlanes (Offset per File)</option>
      <option value="cumulative">Global Cumulative Offset (Proportional)</option>
    </select>
  </div>

  <div class="ctrl-group">
    <label class="chk" title="Clamps tiny reads to at least 3px height and 2px width so metadata/footer reads are always visible">
      <input type="checkbox" id="chk-min-px" checked> Min 3px Box Visibility
    </label>
  </div>
</div>

<!-- Panel 1: Space-Time File & Offset Canvas -->
<div class="panel">
  <div class="panel-header">
    <span class="panel-title">1. Space-Time File &amp; Byte-Offset Map (X = Time, Y = File &amp; Byte Offset [off .. off+len], Color = PID)</span>
    <span class="panel-hint">Drag horizontally/diagonally to zoom • Mouse wheel to zoom time • Hover for exact [offset, len, PID]</span>
  </div>
  <div class="canvas-wrap">
    <canvas id="cv-spacetime" height="460"></canvas>
  </div>
</div>

<!-- Panel 2: Per-PID Concurrency Gantt -->
<div class="panel">
  <div class="panel-header">
    <span class="panel-title">2. Per-PID Activity Timeline (Top half of lane = Wire I/O, Bottom half = Logical Read)</span>
    <span class="panel-hint">Reveals worker startup skew, stragglers, and CPU/collation gaps between batches</span>
  </div>
  <div class="canvas-wrap">
    <canvas id="cv-gantt" height="170"></canvas>
  </div>
</div>

<!-- Panel 3: Bandwidth & IOPS Time-series -->
<div class="panel">
  <div class="panel-header">
    <span class="panel-title">3. Instantaneous Throughput Timeline (Wire MB/s vs. Logical MB/s)</span>
    <span class="panel-hint">Synchronized with time zoom above</span>
  </div>
  <div class="canvas-wrap">
    <canvas id="cv-bw" height="135"></canvas>
  </div>
</div>

<!-- Breakdown Tables -->
<div class="tables-grid">
  <div class="panel">
    <div class="panel-header"><span class="panel-title">Per-PID I/O Breakdown</span></div>
    <div style="overflow-x:auto;">
      <table id="tbl-pids">
        <thead>
          <tr>
            <th>PID / Role</th>
            <th class="num">Files</th>
            <th class="num">Logical Reads</th>
            <th class="num">Logical Bytes</th>
            <th class="num">Wire Fetches</th>
            <th class="num">Wire Bytes</th>
            <th class="num">Read Amp</th>
            <th class="num">Cache Hit %</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="panel">
    <div class="panel-header"><span class="panel-title">Per-File I/O Breakdown &amp; Cross-PID Access</span></div>
    <div style="overflow-x:auto;">
      <table id="tbl-files">
        <thead>
          <tr>
            <th>File Shard</th>
            <th class="num">File Size</th>
            <th>PIDs Reading</th>
            <th class="num">Logical Bytes</th>
            <th class="num">Wire Bytes</th>
            <th class="num">Read Amp</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>

<div id="tooltip"></div>

<script>
const DATA = __TRACE_PAYLOAD_JSON__;

const PALETTE = [
  "#38bdf8", // Sky Blue
  "#fb923c", // Orange
  "#34d399", // Emerald
  "#c084fc", // Purple
  "#f43f5e", // Rose
  "#facc15", // Yellow
  "#2dd4bf", // Teal
  "#818cf8", // Indigo
  "#a3e635", // Lime
  "#e879f9"  // Fuchsia
];

function fmtBytes(b) {
  if (b === null || b === undefined || isNaN(b)) return "0 B";
  const abs = Math.abs(b);
  if (abs >= 1024 * 1024 * 1024) return (b / (1024 * 1024 * 1024)).toFixed(2) + " GiB";
  if (abs >= 1024 * 1024) return (b / (1024 * 1024)).toFixed(2) + " MiB";
  if (abs >= 1024) return (b / 1024).toFixed(1) + " KiB";
  return b + " B";
}

function fmtTime(s) {
  if (s < 0.001) return (s * 1e6).toFixed(0) + " µs";
  if (s < 1.0) return (s * 1e3).toFixed(2) + " ms";
  return s.toFixed(3) + " s";
}

function hexToRgba(hex, alpha) {
  const v = parseInt(hex.slice(1), 16);
  const r = (v >> 16) & 255;
  const g = (v >> 8) & 255;
  const b = v & 255;
  return `rgba(${r},${g},${b},${alpha})`;
}

// Assign colors to PIDs
const pidColorMap = new Map();
const pidRoleMap = new Map();
DATA.pids.forEach((p, idx) => {
  pidColorMap.set(p.pid, PALETTE[idx % PALETTE.length]);
  pidRoleMap.set(p.pid, p.role);
});

const activePids = new Set(DATA.pids.map(p => p.pid));
let showWire = true;
let showLogical = true;
let missesOnly = false;
let fileMode = "swimlanes"; // "swimlanes" | "cumulative" | "single:<path>"
let enforceMinPx = true;

// Compute global time bounds
let minTime = 0.0;
let maxTime = 0.001;
DATA.events.forEach(e => {
  if (e.te > maxTime) maxTime = e.te;
});
maxTime *= 1.02;

let viewT0 = minTime;
let viewT1 = maxTime;
let viewY0 = 0.0; // Normalized 0..1 vertical zoom
let viewY1 = 1.0;

// Populate file dropdown
const selFileMode = document.getElementById("sel-file-mode");
DATA.files.forEach(f => {
  const opt = document.createElement("option");
  opt.value = "single:" + f.path;
  opt.textContent = "Single File: " + f.short + " (" + fmtBytes(f.size) + ")";
  selFileMode.appendChild(opt);
});

document.getElementById("hdr-title").textContent = DATA.title || "Workload I/O Pattern Explorer";

// Build KPI Cards & Summary Tables
function buildSummaries() {
  let logicalBytes = 0, logicalCount = 0, logicalHits = 0;
  let wireBytes = 0, wireCount = 0;

  const perPid = new Map();
  DATA.pids.forEach(p => {
    perPid.set(p.pid, {
      pid: p.pid,
      role: p.role,
      files: new Set(),
      lCount: 0, lBytes: 0, lHits: 0,
      wCount: 0, wBytes: 0
    });
  });

  const perFile = new Map();
  DATA.files.forEach(f => {
    perFile.set(f.path, {
      meta: f,
      pids: new Set(),
      lBytes: 0,
      wBytes: 0
    });
  });

  DATA.events.forEach(e => {
    const pStat = perPid.get(e.pid);
    const fStat = perFile.get(e.file);
    if (pStat) pStat.files.add(e.file);
    if (fStat) fStat.pids.add(e.pid);

    if (e.layer === "logical") {
      logicalCount++;
      logicalBytes += e.len;
      if (e.hit) logicalHits++;
      if (pStat) {
        pStat.lCount++;
        pStat.lBytes += e.len;
        if (e.hit) pStat.lHits++;
      }
      if (fStat) fStat.lBytes += e.len;
    } else if (e.layer === "wire") {
      wireCount++;
      wireBytes += e.len;
      if (pStat) {
        pStat.wCount++;
        pStat.wBytes += e.len;
      }
      if (fStat) fStat.wBytes += e.len;
    }
  });

  const amp = logicalBytes > 0 ? (wireBytes / logicalBytes) : 0;
  const hitRate = logicalCount > 0 ? (100.0 * logicalHits / logicalCount) : 0;
  const totalCorpus = DATA.files.reduce((acc, f) => acc + f.size, 0);

  const kpiGrid = document.getElementById("kpi-grid");
  const ampClass = amp <= 1.15 ? "amp-good" : (amp <= 1.75 ? "amp-warn" : "amp-bad");
  kpiGrid.innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Trace Duration</div>
      <div class="kpi-val">${maxTime.toFixed(3)} s</div>
      <div class="kpi-sub">${DATA.events.length.toLocaleString()} total I/O events</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Active Processes</div>
      <div class="kpi-val">${DATA.pids.length} PIDs</div>
      <div class="kpi-sub">${DATA.files.length} files (${fmtBytes(totalCorpus)})</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Logical Read (Caller)</div>
      <div class="kpi-val">${fmtBytes(logicalBytes)}</div>
      <div class="kpi-sub">${logicalCount.toLocaleString()} read() calls</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Wire Fetched (Storage)</div>
      <div class="kpi-val">${fmtBytes(wireBytes)}</div>
      <div class="kpi-sub">${wireCount.toLocaleString()} range fetches</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Read Amplification</div>
      <div class="kpi-val ${ampClass}">${amp.toFixed(2)}x</div>
      <div class="kpi-sub">Wire Bytes &divide; Logical Bytes</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Buffer Cache Hit Rate</div>
      <div class="kpi-val">${hitRate.toFixed(1)}%</div>
      <div class="kpi-sub">${logicalHits.toLocaleString()} / ${logicalCount.toLocaleString()} logical reads</div>
    </div>
  `;

  // Populate PID table
  const tbodyPids = document.querySelector("#tbl-pids tbody");
  tbodyPids.innerHTML = "";
  perPid.forEach(st => {
    const pAmp = st.lBytes > 0 ? (st.wBytes / st.lBytes) : 0;
    const pHit = st.lCount > 0 ? (100.0 * st.lHits / st.lCount) : 0;
    const color = pidColorMap.get(st.pid);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><span class="pid-dot" style="background:${color};margin-right:6px;"></span><strong>PID ${st.pid}</strong> <span style="color:var(--muted)">(${st.role})</span></td>
      <td class="num">${st.files.size}</td>
      <td class="num">${st.lCount.toLocaleString()}</td>
      <td class="num">${fmtBytes(st.lBytes)}</td>
      <td class="num">${st.wCount.toLocaleString()}</td>
      <td class="num">${fmtBytes(st.wBytes)}</td>
      <td class="num ${pAmp <= 1.15 ? 'amp-good' : (pAmp <= 1.75 ? 'amp-warn' : 'amp-bad')}">${st.lBytes > 0 ? pAmp.toFixed(2) + 'x' : '—'}</td>
      <td class="num">${st.lCount > 0 ? pHit.toFixed(1) + '%' : '—'}</td>
    `;
    tbodyPids.appendChild(tr);
  });

  // Populate Files table
  const tbodyFiles = document.querySelector("#tbl-files tbody");
  tbodyFiles.innerHTML = "";
  perFile.forEach(st => {
    const fAmp = st.lBytes > 0 ? (st.wBytes / st.lBytes) : 0;
    const pidBadges = Array.from(st.pids).map(pid => {
      const c = pidColorMap.get(pid);
      return `<span style="display:inline-flex;align-items:center;gap:4px;padding:1px 6px;border-radius:4px;background:rgba(255,255,255,0.06);margin-right:4px;font-size:11px;"><span class="pid-dot" style="background:${c};width:7px;height:7px;"></span>${pid}</span>`;
    }).join("");
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td title="${st.meta.path}"><strong>${st.meta.short}</strong></td>
      <td class="num">${fmtBytes(st.meta.size)}</td>
      <td>${pidBadges}</td>
      <td class="num">${fmtBytes(st.lBytes)}</td>
      <td class="num">${fmtBytes(st.wBytes)}</td>
      <td class="num ${fAmp <= 1.15 ? 'amp-good' : (fAmp <= 1.75 ? 'amp-warn' : 'amp-bad')}">${st.lBytes > 0 ? fAmp.toFixed(2) + 'x' : '—'}</td>
    `;
    tbodyFiles.appendChild(tr);
  });
}

// Build PID filter pills
const pidPillsWrap = document.getElementById("pid-pills");
function renderPidPills() {
  pidPillsWrap.innerHTML = "";
  DATA.pids.forEach(p => {
    const pill = document.createElement("div");
    pill.className = "pid-pill" + (activePids.has(p.pid) ? "" : " inactive");
    const color = pidColorMap.get(p.pid);
    pill.innerHTML = `<span class="pid-dot" style="background:${color}"></span><span>PID ${p.pid} (${p.role})</span>`;
    pill.addEventListener("click", () => {
      if (activePids.has(p.pid)) {
        if (activePids.size > 1) activePids.delete(p.pid);
      } else {
        activePids.add(p.pid);
      }
      renderPidPills();
      drawAll();
    });
    pidPillsWrap.appendChild(pill);
  });
}

document.getElementById("btn-all-pids").addEventListener("click", () => {
  DATA.pids.forEach(p => activePids.add(p.pid));
  renderPidPills();
  drawAll();
});

document.getElementById("chk-wire").addEventListener("change", e => { showWire = e.target.checked; drawAll(); });
document.getElementById("chk-logical").addEventListener("change", e => { showLogical = e.target.checked; drawAll(); });
document.getElementById("chk-misses-only").addEventListener("change", e => { missesOnly = e.target.checked; drawAll(); });
document.getElementById("chk-min-px").addEventListener("change", e => { enforceMinPx = e.target.checked; drawAll(); });
selFileMode.addEventListener("change", e => {
  fileMode = e.target.value;
  viewY0 = 0.0;
  viewY1 = 1.0;
  drawAll();
});
document.getElementById("btn-reset-zoom").addEventListener("click", () => {
  viewT0 = minTime;
  viewT1 = maxTime;
  viewY0 = 0.0;
  viewY1 = 1.0;
  drawAll();
});

const cvSpaceTime = document.getElementById("cv-spacetime");
const cvGantt = document.getElementById("cv-gantt");
const cvBw = document.getElementById("cv-bw");
const tooltip = document.getElementById("tooltip");

const LEFT_PAD = 185;
const RIGHT_PAD = 20;
const TOP_PAD = 24;
const BOT_PAD = 28;

// Keep hit-test rectangles for hover inspection on Space-Time & Gantt canvases
let hitBoxesSpaceTime = [];
let hitBoxesGantt = [];

function setupCanvas(cv) {
  const dpr = window.devicePixelRatio || 1;
  const rect = cv.getBoundingClientRect();
  cv.width = Math.max(300, Math.floor(rect.width * dpr));
  cv.height = Math.floor(rect.height * dpr);
  const ctx = cv.getContext("2d");
  ctx.scale(dpr, dpr);
  return { ctx, w: rect.width, h: rect.height };
}

function getVisibleFiles() {
  if (fileMode.startsWith("single:")) {
    const target = fileMode.slice("single:".length);
    return DATA.files.filter(f => f.path === target);
  }
  return DATA.files;
}

function drawTimeGrid(ctx, w, h, topPad, botPad) {
  const plotW = w - LEFT_PAD - RIGHT_PAD;
  const plotH = h - topPad - botPad;
  ctx.strokeStyle = "#1e293b";
  ctx.fillStyle = "#94a3b8";
  ctx.font = "11px Inter, monospace";
  ctx.textAlign = "center";

  const nTicks = Math.max(4, Math.floor(plotW / 110));
  for (let i = 0; i <= nTicks; i++) {
    const frac = i / nTicks;
    const t = viewT0 + frac * (viewT1 - viewT0);
    const x = LEFT_PAD + frac * plotW;
    ctx.beginPath();
    ctx.moveTo(x, topPad);
    ctx.lineTo(x, topPad + plotH);
    ctx.stroke();
    ctx.fillText(t.toFixed(3) + "s", x, topPad + plotH + 16);
  }
}

function drawSpaceTime() {
  const { ctx, w, h } = setupCanvas(cvSpaceTime);
  ctx.clearRect(0, 0, w, h);
  hitBoxesSpaceTime = [];

  const plotW = w - LEFT_PAD - RIGHT_PAD;
  const plotH = h - TOP_PAD - BOT_PAD;
  if (plotW <= 10 || plotH <= 10) return;

  drawTimeGrid(ctx, w, h, TOP_PAD, BOT_PAD);

  const visFiles = getVisibleFiles();
  if (visFiles.length === 0) return;

  // Build vertical layout mapping: (filePath, offset, length) -> [normY0, normY1] in 0..1
  const fileLayout = new Map();
  if (fileMode === "cumulative") {
    const totalBytes = visFiles.reduce((acc, f) => acc + f.size, 0) || 1;
    let cur = 0;
    visFiles.forEach(f => {
      const yStart = cur / totalBytes;
      const ySpan = f.size / totalBytes;
      fileLayout.set(f.path, { f, yStart, ySpan });
      cur += f.size;
    });
  } else {
    const laneSpan = 1.0 / visFiles.length;
    const gap = visFiles.length > 1 ? Math.min(0.015, laneSpan * 0.08) : 0;
    visFiles.forEach((f, idx) => {
      const yStart = idx * laneSpan + gap * 0.5;
      const ySpan = laneSpan - gap;
      fileLayout.set(f.path, { f, yStart, ySpan });
    });
  }

  // Draw File Swimlane Backgrounds & Y-Axis Labels
  ctx.save();
  ctx.beginPath();
  ctx.rect(0, TOP_PAD, w, plotH);
  ctx.clip();

  fileLayout.forEach(({ f, yStart, ySpan }) => {
    // Convert normalized [yStart, yStart + ySpan] through [viewY0, viewY1]
    const ny0 = (yStart - viewY0) / (viewY1 - viewY0);
    const ny1 = (yStart + ySpan - viewY0) / (viewY1 - viewY0);
    // In each lane, offset=0 is at the bottom of the lane, offset=file_size is at the top of the lane!
    const pyTop = TOP_PAD + ny0 * plotH;
    const pyBot = TOP_PAD + ny1 * plotH;
    const laneH = pyBot - pyTop;
    if (pyBot < TOP_PAD || pyTop > TOP_PAD + plotH) return;

    ctx.fillStyle = "rgba(30, 41, 59, 0.38)";
    ctx.fillRect(LEFT_PAD, pyTop, plotW, laneH);
    ctx.strokeStyle = "#334155";
    ctx.strokeRect(LEFT_PAD, pyTop, plotW, laneH);

    // Label on the left margin
    ctx.fillStyle = "#e2e8f0";
    ctx.font = "600 11px Inter, monospace";
    ctx.textAlign = "right";
    const midY = Math.max(TOP_PAD + 12, Math.min(TOP_PAD + plotH - 12, (pyTop + pyBot) / 2));
    ctx.fillText(f.short, LEFT_PAD - 8, midY - 2);
    ctx.fillStyle = "#64748b";
    ctx.font = "10px Inter, monospace";
    ctx.fillText("0 .. " + fmtBytes(f.size), LEFT_PAD - 8, midY + 11);
  });
  ctx.restore();

  // Clip to plot area for events
  ctx.save();
  ctx.beginPath();
  ctx.rect(LEFT_PAD, TOP_PAD, plotW, plotH);
  ctx.clip();

  const tSpan = Math.max(1e-9, viewT1 - viewT0);
  const yZoomSpan = Math.max(1e-9, viewY1 - viewY0);

  // Draw Wire events first, then Logical events on top so Logical sits crisply inside Wire boxes
  for (let pass = 0; pass < 2; pass++) {
    const targetLayer = pass === 0 ? "wire" : "logical";
    if (targetLayer === "wire" && !showWire) continue;
    if (targetLayer === "logical" && !showLogical) continue;

    for (let i = 0; i < DATA.events.length; i++) {
      const ev = DATA.events[i];
      if (ev.layer !== targetLayer) continue;
      if (!activePids.has(ev.pid)) continue;
      if (targetLayer === "logical" && missesOnly && ev.hit) continue;

      const layout = fileLayout.get(ev.file);
      if (!layout) continue;
      if (ev.te < viewT0 || ev.ts > viewT1) continue;

      const x0 = LEFT_PAD + ((ev.ts - viewT0) / tSpan) * plotW;
      const x1 = LEFT_PAD + ((ev.te - viewT0) / tSpan) * plotW;
      let bw = x1 - x0;
      if (enforceMinPx && bw < 2.5) bw = 2.5;

      // Byte offset within file: 0 at bottom of lane (yStart + ySpan), f.size at top of lane (yStart)
      const fsize = Math.max(1, layout.f.size);
      const fracStart = Math.max(0, Math.min(1, ev.off / fsize));
      const fracEnd = Math.max(0, Math.min(1, (ev.off + ev.len) / fsize));

      const normYTop = layout.yStart + (1.0 - fracEnd) * layout.ySpan;
      const normYBot = layout.yStart + (1.0 - fracStart) * layout.ySpan;

      const pyTop = TOP_PAD + ((normYTop - viewY0) / yZoomSpan) * plotH;
      const pyBot = TOP_PAD + ((normYBot - viewY0) / yZoomSpan) * plotH;
      let bh = pyBot - pyTop;
      let drawY = pyTop;
      if (enforceMinPx && bh < 3.0) {
        drawY = (pyTop + pyBot) * 0.5 - 1.5;
        bh = 3.0;
      }

      const color = pidColorMap.get(ev.pid) || "#38bdf8";
      if (targetLayer === "wire") {
        ctx.fillStyle = hexToRgba(color, 0.22);
        ctx.fillRect(x0, drawY, bw, bh);
        ctx.strokeStyle = hexToRgba(color, 0.85);
        ctx.lineWidth = 1.2;
        ctx.setLineDash([3, 2]);
        ctx.strokeRect(x0, drawY, bw, bh);
        ctx.setLineDash([]);
      } else {
        ctx.fillStyle = hexToRgba(color, 0.92);
        ctx.fillRect(x0, drawY, bw, bh);
        if (!ev.hit) {
          ctx.strokeStyle = "#ffffff";
          ctx.lineWidth = 0.8;
          ctx.strokeRect(x0, drawY, bw, bh);
        }
      }

      hitBoxesSpaceTime.push({
        x: x0,
        y: drawY,
        w: Math.max(bw, 4),
        h: Math.max(bh, 4),
        ev
      });
    }
  }

  ctx.restore();
}

function drawGantt() {
  const { ctx, w, h } = setupCanvas(cvGantt);
  ctx.clearRect(0, 0, w, h);
  hitBoxesGantt = [];

  const plotW = w - LEFT_PAD - RIGHT_PAD;
  const plotH = h - 14 - BOT_PAD;
  const topPad = 14;
  if (plotW <= 10 || plotH <= 10) return;

  drawTimeGrid(ctx, w, h, topPad, BOT_PAD);

  const pids = DATA.pids.filter(p => activePids.has(p.pid));
  if (pids.length === 0) return;

  const laneH = plotH / pids.length;
  const pidIndex = new Map();
  pids.forEach((p, idx) => {
    pidIndex.set(p.pid, idx);
    const y0 = topPad + idx * laneH;
    ctx.fillStyle = idx % 2 === 0 ? "rgba(30, 41, 59, 0.35)" : "rgba(15, 23, 42, 0.35)";
    ctx.fillRect(LEFT_PAD, y0, plotW, laneH);
    ctx.strokeStyle = "#334155";
    ctx.strokeRect(LEFT_PAD, y0, plotW, laneH);

    const color = pidColorMap.get(p.pid);
    ctx.fillStyle = color;
    ctx.font = "600 11px Inter, monospace";
    ctx.textAlign = "right";
    ctx.fillText(`PID ${p.pid} (${p.role})`, LEFT_PAD - 8, y0 + laneH * 0.5 + 4);
  });

  ctx.save();
  ctx.beginPath();
  ctx.rect(LEFT_PAD, topPad, plotW, plotH);
  ctx.clip();

  const tSpan = Math.max(1e-9, viewT1 - viewT0);
  for (let i = 0; i < DATA.events.length; i++) {
    const ev = DATA.events[i];
    const idx = pidIndex.get(ev.pid);
    if (idx === undefined) continue;
    if (ev.layer === "wire" && !showWire) continue;
    if (ev.layer === "logical" && !showLogical) continue;
    if (ev.te < viewT0 || ev.ts > viewT1) continue;

    const x0 = LEFT_PAD + ((ev.ts - viewT0) / tSpan) * plotW;
    const x1 = LEFT_PAD + ((ev.te - viewT0) / tSpan) * plotW;
    const bw = Math.max(2.0, x1 - x0);

    const laneTop = topPad + idx * laneH;
    const color = pidColorMap.get(ev.pid);
    let y, bh;
    if (ev.layer === "wire") {
      y = laneTop + 3;
      bh = Math.max(4, laneH * 0.44 - 4);
      ctx.fillStyle = hexToRgba(color, 0.45);
      ctx.fillRect(x0, y, bw, bh);
      ctx.strokeStyle = color;
      ctx.strokeRect(x0, y, bw, bh);
    } else {
      y = laneTop + laneH * 0.52;
      bh = Math.max(4, laneH * 0.44 - 4);
      ctx.fillStyle = hexToRgba(color, 0.95);
      ctx.fillRect(x0, y, bw, bh);
    }
    hitBoxesGantt.push({ x: x0, y, w: Math.max(bw, 4), h: bh, ev });
  }
  ctx.restore();
}

function drawBandwidth() {
  const { ctx, w, h } = setupCanvas(cvBw);
  ctx.clearRect(0, 0, w, h);

  const topPad = 14;
  const plotW = w - LEFT_PAD - RIGHT_PAD;
  const plotH = h - topPad - BOT_PAD;
  if (plotW <= 10 || plotH <= 10) return;

  drawTimeGrid(ctx, w, h, topPad, BOT_PAD);

  const nBins = 120;
  const wireBins = new Float64Array(nBins);
  const logicalBins = new Float64Array(nBins);
  const tSpan = Math.max(1e-9, viewT1 - viewT0);
  const binDur = tSpan / nBins;

  DATA.events.forEach(ev => {
    if (!activePids.has(ev.pid)) return;
    if (ev.te < viewT0 || ev.ts > viewT1) return;
    const s = Math.max(viewT0, ev.ts);
    const e = Math.min(viewT1, Math.max(ev.ts + 1e-6, ev.te));
    const dur = Math.max(1e-6, ev.te - ev.ts);
    const rate = (ev.len / (1024 * 1024)) / dur; // MiB/s

    const b0 = Math.max(0, Math.floor(((s - viewT0) / tSpan) * nBins));
    const b1 = Math.min(nBins - 1, Math.floor(((e - viewT0) / tSpan) * nBins));
    for (let b = b0; b <= b1; b++) {
      const bs = viewT0 + b * binDur;
      const be = bs + binDur;
      const overlap = Math.max(0, Math.min(e, be) - Math.max(s, bs));
      const contrib = rate * (overlap / binDur);
      if (ev.layer === "wire") wireBins[b] += contrib;
      else if (ev.layer === "logical") logicalBins[b] += contrib;
    }
  });

  let maxBw = 1.0;
  for (let i = 0; i < nBins; i++) {
    if (showWire && wireBins[i] > maxBw) maxBw = wireBins[i];
    if (showLogical && logicalBins[i] > maxBw) maxBw = logicalBins[i];
  }
  maxBw *= 1.15;

  // Y axis labels
  ctx.fillStyle = "#94a3b8";
  ctx.font = "10px Inter, monospace";
  ctx.textAlign = "right";
  ctx.fillText(maxBw.toFixed(1) + " MiB/s", LEFT_PAD - 8, topPad + 10);
  ctx.fillText((maxBw * 0.5).toFixed(1) + " MiB/s", LEFT_PAD - 8, topPad + plotH * 0.5 + 4);
  ctx.fillText("0 MiB/s", LEFT_PAD - 8, topPad + plotH);

  function plotSeries(bins, strokeColor, fillColor) {
    ctx.beginPath();
    ctx.moveTo(LEFT_PAD, topPad + plotH);
    for (let i = 0; i < nBins; i++) {
      const x = LEFT_PAD + ((i + 0.5) / nBins) * plotW;
      const y = topPad + plotH - (bins[i] / maxBw) * plotH;
      ctx.lineTo(x, y);
    }
    ctx.lineTo(LEFT_PAD + plotW, topPad + plotH);
    ctx.closePath();
    ctx.fillStyle = fillColor;
    ctx.fill();

    ctx.beginPath();
    for (let i = 0; i < nBins; i++) {
      const x = LEFT_PAD + ((i + 0.5) / nBins) * plotW;
      const y = topPad + plotH - (bins[i] / maxBw) * plotH;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = 1.5;
    ctx.stroke();
  }

  if (showWire) plotSeries(wireBins, "#38bdf8", "rgba(56, 189, 248, 0.18)");
  if (showLogical) plotSeries(logicalBins, "#34d399", "rgba(52, 211, 153, 0.18)");
}

function drawAll() {
  drawSpaceTime();
  drawGantt();
  drawBandwidth();
}

// Hover Tooltip & Drag-to-Zoom Interaction
function attachInteractions(cv, getHits, allowVerticalZoom) {
  let dragging = false;
  let dragX0 = 0, dragY0 = 0, dragX1 = 0, dragY1 = 0;

  cv.addEventListener("mousemove", e => {
    const rect = cv.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;

    if (dragging) {
      dragX1 = mx;
      dragY1 = my;
      drawAll();
      const ctx = cv.getContext("2d");
      ctx.save();
      ctx.fillStyle = "rgba(56, 189, 248, 0.18)";
      ctx.strokeStyle = "#38bdf8";
      ctx.lineWidth = 1;
      const rx = Math.min(dragX0, dragX1);
      const rw = Math.abs(dragX1 - dragX0);
      const ry = allowVerticalZoom ? Math.min(dragY0, dragY1) : TOP_PAD;
      const rh = allowVerticalZoom ? Math.abs(dragY1 - dragY0) : (rect.height - TOP_PAD - BOT_PAD);
      ctx.fillRect(rx, ry, rw, rh);
      ctx.strokeRect(rx, ry, rw, rh);
      ctx.restore();
      return;
    }

    const hits = getHits();
    // Search top-most (last drawn) first
    let found = null;
    for (let i = hits.length - 1; i >= 0; i--) {
      const b = hits[i];
      if (mx >= b.x - 1 && mx <= b.x + b.w + 1 && my >= b.y - 1 && my <= b.y + b.h + 1) {
        found = b.ev;
        break;
      }
    }

    if (found) {
      const color = pidColorMap.get(found.pid) || "#38bdf8";
      const role = pidRoleMap.get(found.pid) || "";
      const dur = Math.max(1e-6, found.te - found.ts);
      const bw = (found.len / (1024 * 1024)) / dur;
      const pct = found.fsize > 0 ? ((100.0 * found.off) / found.fsize).toFixed(1) + "%" : "";
      const layerBadge = found.layer === "wire"
        ? `<span class="tt-badge" style="background:rgba(56,189,248,0.2);color:#38bdf8;border:1px solid #38bdf8;">WIRE FETCH (${found.op})</span>`
        : `<span class="tt-badge" style="background:${found.hit ? 'rgba(74,222,128,0.2)' : 'rgba(251,146,60,0.25)'};color:${found.hit ? '#4ade80' : '#fb923c'};border:1px solid ${found.hit ? '#4ade80' : '#fb923c'};">LOGICAL READ (${found.hit ? 'CACHE HIT' : 'CACHE MISS'})</span>`;

      tooltip.style.display = "block";
      tooltip.style.left = Math.min(window.innerWidth - 390, e.clientX + 14) + "px";
      tooltip.style.top = Math.min(window.innerHeight - 230, e.clientY + 14) + "px";
      tooltip.innerHTML = `
        <div class="tt-head">
          <span><span class="pid-dot" style="background:${color};margin-right:6px;"></span>PID ${found.pid} (${role})</span>
          ${layerBadge}
        </div>
        <div class="tt-row"><span class="tt-k">File:</span><span class="tt-v">${found.file.split('/').slice(-2).join('/')}</span></div>
        <div class="tt-row"><span class="tt-k">File Size:</span><span class="tt-v">${fmtBytes(found.fsize)}</span></div>
        <div class="tt-row"><span class="tt-k">Offset:</span><span class="tt-v">${found.off.toLocaleString()} B (${fmtBytes(found.off)} ${pct ? '• ' + pct : ''})</span></div>
        <div class="tt-row"><span class="tt-k">Length:</span><span class="tt-v">${found.len.toLocaleString()} B (${fmtBytes(found.len)})</span></div>
        <div class="tt-row"><span class="tt-k">End Offset:</span><span class="tt-v">${(found.off + found.len).toLocaleString()} B (${fmtBytes(found.off + found.len)})</span></div>
        <div class="tt-row"><span class="tt-k">Time Window:</span><span class="tt-v">${found.ts.toFixed(4)}s &rarr; ${found.te.toFixed(4)}s</span></div>
        <div class="tt-row"><span class="tt-k">Duration / BW:</span><span class="tt-v">${fmtTime(dur)} (${bw.toFixed(1)} MiB/s)</span></div>
        <div class="tt-row"><span class="tt-k">Thread:</span><span class="tt-v">${found.tname || found.tid}</span></div>
      `;
    } else {
      tooltip.style.display = "none";
    }
  });

  cv.addEventListener("mouseleave", () => {
    tooltip.style.display = "none";
    if (dragging) {
      dragging = false;
      drawAll();
    }
  });

  cv.addEventListener("mousedown", e => {
    const rect = cv.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    if (mx < LEFT_PAD) return;
    dragging = true;
    dragX0 = dragX1 = mx;
    dragY0 = dragY1 = my;
  });

  cv.addEventListener("mouseup", e => {
    if (!dragging) return;
    dragging = false;
    const rect = cv.getBoundingClientRect();
    const plotW = rect.width - LEFT_PAD - RIGHT_PAD;
    const plotH = rect.height - TOP_PAD - BOT_PAD;
    if (Math.abs(dragX1 - dragX0) > 8) {
      const f0 = Math.max(0, Math.min(1, (Math.min(dragX0, dragX1) - LEFT_PAD) / plotW));
      const f1 = Math.max(0, Math.min(1, (Math.max(dragX0, dragX1) - LEFT_PAD) / plotW));
      const newT0 = viewT0 + f0 * (viewT1 - viewT0);
      const newT1 = viewT0 + f1 * (viewT1 - viewT0);
      if (newT1 - newT0 > 1e-5) {
        viewT0 = newT0;
        viewT1 = newT1;
      }
    }
    if (allowVerticalZoom && Math.abs(dragY1 - dragY0) > 12) {
      const fy0 = Math.max(0, Math.min(1, (Math.min(dragY0, dragY1) - TOP_PAD) / plotH));
      const fy1 = Math.max(0, Math.min(1, (Math.max(dragY0, dragY1) - TOP_PAD) / plotH));
      const newY0 = viewY0 + fy0 * (viewY1 - viewY0);
      const newY1 = viewY0 + fy1 * (viewY1 - viewY0);
      if (newY1 - newY0 > 1e-4) {
        viewY0 = newY0;
        viewY1 = newY1;
      }
    }
    drawAll();
  });

  cv.addEventListener("wheel", e => {
    e.preventDefault();
    const rect = cv.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    if (mx < LEFT_PAD) return;
    const plotW = rect.width - LEFT_PAD - RIGHT_PAD;
    const frac = Math.max(0, Math.min(1, (mx - LEFT_PAD) / plotW));
    const factor = e.deltaY < 0 ? 0.8 : 1.25;
    const curSpan = viewT1 - viewT0;
    const newSpan = Math.min(maxTime - minTime, Math.max(1e-4, curSpan * factor));
    const anchor = viewT0 + frac * curSpan;
    viewT0 = Math.max(minTime, anchor - frac * newSpan);
    viewT1 = Math.min(maxTime, viewT0 + newSpan);
    drawAll();
  }, { passive: false });
}

attachInteractions(cvSpaceTime, () => hitBoxesSpaceTime, true);
attachInteractions(cvGantt, () => hitBoxesGantt, false);
attachInteractions(cvBw, () => [], false);

window.addEventListener("resize", drawAll);

buildSummaries();
renderPidPills();
drawAll();
</script>
</body>
</html>
"""
