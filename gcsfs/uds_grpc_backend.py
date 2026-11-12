"""Pure-Python `grpc.aio` UDS sidecar client adapter for `gcsfs` (Scenario 3)."""

from __future__ import annotations

import asyncio
import base64
import itertools
import os
import threading
from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

from gcsfs._proto import filesystem_pb2 as pb2
from gcsfs._proto import filesystem_pb2_grpc as pb2_grpc
from gcsfs.rust_backend import _finalize_info_dict

if TYPE_CHECKING:
    from gcsfs.core import GCSFileSystem

_MAX_MSG_BYTES = 16 * 1024 * 1024
_WRITE_CHUNK_BYTES = 8 * 1024 * 1024
_READ_CHUNK_BYTES = 8 * 1024 * 1024
_RANDOM_READ_FASTPATH_MAX = 4 * 1024 * 1024

_CHANNEL_OPTIONS = [
    ("grpc.default_authority", "localhost"),
    ("grpc.max_receive_message_length", _MAX_MSG_BYTES),
    ("grpc.max_send_message_length", _MAX_MSG_BYTES),
    ("grpc.http2.max_frame_size", 4 * 1024 * 1024),
    ("grpc.http2.lookahead_bytes", _MAX_MSG_BYTES),
]


class _PidLoopPool:
    """Per-PID background asyncio event loop and pooled `grpc.aio` UDS channels."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stubs: dict[str, list[pb2_grpc.FileSystemServiceStub]] = {}
        self._rr = itertools.count()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        pid = os.getpid()
        if self._pid == pid and self._loop is not None and self._loop.is_running():
            return self._loop
        with self._lock:
            if self._pid == pid and self._loop is not None and self._loop.is_running():
                return self._loop
            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _runner() -> None:
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()

            thread = threading.Thread(
                target=_runner, name=f"gcsfs-uds-grpc-{pid}", daemon=True
            )
            thread.start()
            ready.wait()
            self._pid = pid
            self._loop = loop
            self._thread = thread
            self._stubs = {}
            return loop

    def run_sync(self, coro):
        loop = self._ensure_loop()
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        return fut.result()

    async def get_stub(self, uds_path: str) -> pb2_grpc.FileSystemServiceStub:
        pool_size = int(os.environ.get("GCS_RUST_FS_UDS_POOL_SIZE", "16"))
        stubs = self._stubs.get(uds_path)
        if stubs is None:
            target = (
                uds_path
                if uds_path.startswith("unix:")
                else f"unix://{os.path.abspath(uds_path)}"
            )
            stubs = []
            for i in range(max(1, pool_size)):
                opts = list(_CHANNEL_OPTIONS) + [("grpc.channel_pool_id", i)]
                ch = grpc.aio.insecure_channel(target, options=opts)
                stubs.append(pb2_grpc.FileSystemServiceStub(ch))
            self._stubs[uds_path] = stubs
        idx = next(self._rr) % len(stubs)
        return stubs[idx]


_POOL = _PidLoopPool()


def resolve_uds_path(fs: GCSFileSystem) -> str:
    path = getattr(fs, "uds_socket_path", None) or os.environ.get(
        "GCSFS_UDS_SOCKET_PATH"
    )
    if not path:
        raise ValueError("use_uds_grpc=True requires uds_socket_path")
    return path


def _format_path_with_generation(path: str, generation: str | None) -> str:
    if generation and "#" not in path:
        return f"{path}#{generation}"
    return path


def _translate_grpc_error(err: grpc.aio.AioRpcError, path: str = "") -> Exception:
    code = err.code()
    details = err.details() or str(err)
    msg = f"{details} ({path})" if path and path not in details else details
    if code == grpc.StatusCode.NOT_FOUND:
        return FileNotFoundError(msg)
    if code == grpc.StatusCode.ALREADY_EXISTS:
        return FileExistsError(msg)
    if code in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAUTHENTICATED):
        return PermissionError(msg)
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return TimeoutError(msg)
    if code == grpc.StatusCode.UNIMPLEMENTED:
        return NotImplementedError(msg)
    if code in (
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.OUT_OF_RANGE,
        grpc.StatusCode.FAILED_PRECONDITION,
    ):
        lower = details.lower()
        if "is a directory" in lower:
            return IsADirectoryError(msg)
        if "not a directory" in lower:
            return NotADirectoryError(msg)
        return ValueError(msg)
    return OSError(msg)


def _entry_pb_to_info_dict(
    fs: GCSFileSystem, entry: pb2.Entry
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "name": entry.path,
        "size": int(entry.size),
    }
    if entry.kind == pb2.ENTRY_KIND_DIRECTORY:
        out["type"] = "directory"
        out["storageClass"] = "DIRECTORY"
        out["bucket"] = entry.path.split("/", 1)[0]
    else:
        out["type"] = "file"
        if entry.HasField("stat"):
            st = entry.stat
            out["bucket"] = (
                st.bucket if st.bucket else entry.path.split("/", 1)[0]
            )
            out["generation"] = str(st.generation)
            out["metageneration"] = str(st.metageneration)
            if st.content_type:
                out["contentType"] = st.content_type
            if st.HasField("content_encoding"):
                out["contentEncoding"] = st.content_encoding
            if st.HasField("cache_control"):
                out["cacheControl"] = st.cache_control
            if st.HasField("content_disposition"):
                out["contentDisposition"] = st.content_disposition
            if st.HasField("content_language"):
                out["contentLanguage"] = st.content_language
            if st.storage_class:
                out["storageClass"] = st.storage_class
            if st.etag:
                out["etag"] = st.etag
            if st.HasField("crc32c"):
                out["crc32c"] = base64.b64encode(
                    int(st.crc32c).to_bytes(4, "big")
                ).decode("ascii")
            if st.HasField("md5_hash"):
                out["md5Hash"] = st.md5_hash
            if st.HasField("kms_key"):
                out["kmsKeyName"] = st.kms_key
            if st.HasField("component_count"):
                out["componentCount"] = int(st.component_count)
            if st.HasField("time_created"):
                out["timeCreated"] = st.time_created
            if st.HasField("updated"):
                out["updated"] = st.updated
            if st.HasField("custom_time"):
                out["customTime"] = st.custom_time
            if st.metadata:
                out["metadata"] = dict(st.metadata)
    return _finalize_info_dict(fs, out)


def uds_info_sync(
    fs: GCSFileSystem, path: str, generation: str | None = None
) -> dict[str, Any]:
    uds_path = resolve_uds_path(fs)
    target = _format_path_with_generation(path, generation)

    async def _op() -> dict[str, Any]:
        stub = await _POOL.get_stub(uds_path)
        try:
            resp = await stub.Info(pb2.InfoRequest(path=target))
        except grpc.aio.AioRpcError as err:
            raise _translate_grpc_error(err, target) from None
        return _entry_pb_to_info_dict(fs, resp.entry)

    return _POOL.run_sync(_op())


def uds_cat_file_sync(
    fs: GCSFileSystem,
    path: str,
    start: int | None = None,
    end: int | None = None,
    generation: str | None = None,
) -> bytes:
    uds_path = resolve_uds_path(fs)
    target = _format_path_with_generation(path, generation)

    async def _op() -> bytes:
        stub = await _POOL.get_stub(uds_path)
        byte_range = pb2.ByteRange()
        if start is not None:
            byte_range.start = int(start)
        if end is not None:
            byte_range.end = int(end)
        req = pb2.ReadRequest(
            path=target,
            range=byte_range,
            options=pb2.ReadOptions(chunk_size=_READ_CHUNK_BYTES),
        )
        chunks: list[bytes] = []
        try:
            call = stub.Read(req)
            while True:
                msg = await call.read()
                if msg is grpc.aio.EOF:
                    break
                if msg.data:
                    chunks.append(msg.data)
        except grpc.aio.AioRpcError as err:
            raise _translate_grpc_error(err, target) from None
        return b"".join(chunks) if len(chunks) != 1 else chunks[0]

    return _POOL.run_sync(_op())


def uds_pipe_file_sync(
    fs: GCSFileSystem,
    path: str,
    data: bytes | bytearray | memoryview,
    content_type: str | None = None,
    metadata: dict[str, Any] | None = None,
    mode: str = "overwrite",
) -> None:
    uds_path = resolve_uds_path(fs)
    mv = memoryview(data)
    write_mode = (
        pb2.WRITE_MODE_CREATE
        if mode in ("create", "xb", "x")
        else pb2.WRITE_MODE_OVERWRITE
    )
    meta_map = (
        {str(k): str(v) for k, v in metadata.items() if v is not None}
        if metadata
        else {}
    )

    async def _req_iter():
        spec = pb2.WriteSpec(path=path, mode=write_mode, metadata=meta_map)
        if content_type:
            spec.content_type = content_type
        yield pb2.WriteRequest(spec=spec)
        total = len(mv)
        offset = 0
        while offset < total:
            end = min(offset + _WRITE_CHUNK_BYTES, total)
            yield pb2.WriteRequest(data=bytes(mv[offset:end]))
            offset = end

    async def _op() -> None:
        stub = await _POOL.get_stub(uds_path)
        try:
            await stub.Write(_req_iter())
        except grpc.aio.AioRpcError as err:
            raise _translate_grpc_error(err, path) from None

    _POOL.run_sync(_op())


class UdsGrpcFile:
    """Stream-bound synchronous file handle backed by `grpc.aio` over UDS."""

    def __init__(
        self,
        fs: GCSFileSystem,
        path: str,
        mode: str = "rb",
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        block_size: int | None = None,
        generation: str | None = None,
    ) -> None:
        self._fs = fs
        self._uds_path = resolve_uds_path(fs)
        self._path = path
        self._mode = mode
        self._closed = False
        self._pos = 0
        self._seeked = False
        self._stream_call: Any = None
        self._stream_buf = bytearray()
        self._details: dict[str, Any] | None = None
        self._size: int | None = None
        self._pinned_path = _format_path_with_generation(path, generation)
        self._write_queue: asyncio.Queue[bytes | None] | None = None
        self._write_task: asyncio.Task[Any] | None = None

        if "r" in mode:
            det = uds_info_sync(fs, path, generation=generation)
            if det.get("type") == "directory":
                raise IsADirectoryError(f"Is a directory: {path}")
            self._details = det
            self._size = int(det.get("size", 0))
            gen = det.get("generation")
            if gen and "#" not in self._pinned_path:
                self._pinned_path = f"{path}#{gen}"
        else:
            write_mode = (
                pb2.WRITE_MODE_CREATE
                if mode in ("create", "xb", "x")
                else pb2.WRITE_MODE_OVERWRITE
            )
            meta_map = (
                {str(k): str(v) for k, v in metadata.items() if v is not None}
                if metadata
                else {}
            )

            async def _init_writer() -> None:
                stub = await _POOL.get_stub(self._uds_path)
                q: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=4)
                self._write_queue = q

                async def _gen():
                    spec = pb2.WriteSpec(
                        path=path, mode=write_mode, metadata=meta_map
                    )
                    if content_type:
                        spec.content_type = content_type
                    if block_size and block_size > 0:
                        spec.block_size = int(block_size)
                    yield pb2.WriteRequest(spec=spec)
                    while True:
                        chunk = await q.get()
                        if chunk is None:
                            break
                        yield pb2.WriteRequest(data=chunk)

                async def _run_write():
                    try:
                        return await stub.Write(_gen())
                    except grpc.aio.AioRpcError as err:
                        raise _translate_grpc_error(err, path) from None

                self._write_task = asyncio.create_task(_run_write())

            _POOL.run_sync(_init_writer())

    def info(self) -> dict[str, Any] | None:
        return self._details

    def size(self) -> int | None:
        return self._size

    def tell(self) -> int:
        return self._pos

    def seek(self, pos: int, whence: int = 0) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if whence == 0:
            target = int(pos)
        elif whence == 1:
            target = self._pos + int(pos)
        elif whence == 2:
            if self._size is None:
                raise ValueError("Cannot seek from end on file of unknown size")
            target = self._size + int(pos)
        else:
            raise ValueError(f"Invalid whence: {whence}")
        if target < 0:
            raise ValueError("Negative seek position")
        if target == self._pos:
            return self._pos

        async def _cancel_stream() -> None:
            if self._stream_call is not None:
                try:
                    self._stream_call.cancel()
                except Exception:
                    pass
                self._stream_call = None
            self._stream_buf.clear()

        _POOL.run_sync(_cancel_stream())
        self._pos = target
        self._seeked = True
        return self._pos

    def read(self, size: int | None = None) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if self._size is not None and self._pos >= self._size:
            return b""
        if size == 0:
            return b""

        if size is None or size < 0:
            want = (self._size - self._pos) if self._size is not None else None
        else:
            want = (
                min(int(size), self._size - self._pos)
                if self._size is not None
                else int(size)
            )
        if want == 0:
            return b""

        async def _read_op() -> bytes:
            # Post-seek small random read: fetch exact range [pos, pos + want) on a fresh stream
            # so sidecar's <= 4 MiB fast path executes without readahead amplification.
            if (
                self._seeked
                and want is not None
                and want <= _RANDOM_READ_FASTPATH_MAX
                and self._stream_call is None
            ):
                self._seeked = False
                stub = await _POOL.get_stub(self._uds_path)
                req = pb2.ReadRequest(
                    path=self._pinned_path,
                    range=pb2.ByteRange(
                        start=self._pos, end=self._pos + want
                    ),
                    options=pb2.ReadOptions(
                        chunk_size=min(want, _READ_CHUNK_BYTES)
                    ),
                )
                chunks: list[bytes] = []
                try:
                    call = stub.Read(req)
                    while True:
                        msg = await call.read()
                        if msg is grpc.aio.EOF:
                            break
                        if msg.data:
                            chunks.append(msg.data)
                except grpc.aio.AioRpcError as err:
                    raise _translate_grpc_error(err, self._pinned_path) from None
                data = b"".join(chunks) if len(chunks) != 1 else chunks[0]
                self._pos += len(data)
                return data

            self._seeked = False
            if self._stream_call is None:
                stub = await _POOL.get_stub(self._uds_path)
                byte_range = pb2.ByteRange(start=self._pos)
                if self._size is not None:
                    byte_range.end = self._size
                req = pb2.ReadRequest(
                    path=self._pinned_path,
                    range=byte_range,
                    options=pb2.ReadOptions(chunk_size=_READ_CHUNK_BYTES),
                )
                self._stream_call = stub.Read(req)

            try:
                while want is None or len(self._stream_buf) < want:
                    msg = await self._stream_call.read()
                    if msg is grpc.aio.EOF:
                        self._stream_call = None
                        break
                    if msg.data:
                        self._stream_buf.extend(msg.data)
            except grpc.aio.AioRpcError as err:
                self._stream_call = None
                raise _translate_grpc_error(err, self._pinned_path) from None

            take = (
                len(self._stream_buf)
                if want is None
                else min(want, len(self._stream_buf))
            )
            if (
                take == len(self._stream_buf)
                and self._pos == 0
                and want == self._size
            ):
                out = bytes(self._stream_buf)
                self._stream_buf.clear()
            else:
                out = bytes(self._stream_buf[:take])
                del self._stream_buf[:take]
            self._pos += len(out)
            return out

        return _POOL.run_sync(_read_op())

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if self._write_queue is None:
            raise ValueError("File not open for writing")
        mv = memoryview(data)
        total = len(mv)
        if total == 0:
            return 0

        async def _write_op() -> int:
            assert self._write_queue is not None
            offset = 0
            while offset < total:
                if self._write_task is not None and self._write_task.done():
                    await self._write_task
                end = min(offset + _WRITE_CHUNK_BYTES, total)
                await self._write_queue.put(bytes(mv[offset:end]))
                offset = end
            return total

        written = _POOL.run_sync(_write_op())
        self._pos += written
        return written

    def flush(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file.")

    def close(self) -> None:
        if self._closed:
            return

        async def _close_op() -> None:
            if self._stream_call is not None:
                try:
                    self._stream_call.cancel()
                except Exception:
                    pass
                self._stream_call = None
            self._stream_buf.clear()
            if self._write_queue is not None and self._write_task is not None:
                await self._write_queue.put(None)
                await self._write_task
                self._write_queue = None
                self._write_task = None

        try:
            _POOL.run_sync(_close_op())
        finally:
            self._closed = True

