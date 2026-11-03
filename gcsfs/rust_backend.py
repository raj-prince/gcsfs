"""PyO3 bridge integration for ``gcs-rust-fs`` in ``gcsfs``.

Provides ``info``, ``read`` (``cat_file`` and ``RustGcsFile`` streaming/range reads),
and ``write`` (``pipe_file`` and ``RustGcsFile`` streaming writes) backed by the
pure-Rust ``gcs-rust-fs`` crate via ``gcs_rust_fs_py``.
"""

from __future__ import annotations

import base64
from hashlib import md5
from typing import TYPE_CHECKING, Any

from .checkers import get_consistency_checker

if TYPE_CHECKING:
    from .core import GCSFileSystem

try:
    import gcs_rust_fs_py
except ImportError:  # pragma: no cover
    gcs_rust_fs_py = None


def _require_rust_bridge():
    if gcs_rust_fs_py is None:
        raise ImportError(
            "gcs_rust_fs_py is not installed. Build it with "
            "`maturin develop --release --manifest-path rust/gcs_rust_fs_py/Cargo.toml`."
        )
    return gcs_rust_fs_py


def _resolve_rust_write_mode(mode: str, if_generation_match: int | None) -> str:
    """Map gcsfs write mode and generation preconditions to gcs-rust-fs WriteMode.

    Note: ``gcs-rust-fs`` v0.1.0 exposes ``WriteMode::Overwrite`` and
    ``WriteMode::Create`` (which maps to ``if_generation_match(0)``), but does
    not expose arbitrary ``if_generation_match(N)`` for ``N > 0`` on
    ``WriteOptions``.
    """
    if if_generation_match not in (None, 0):
        raise NotImplementedError(
            "gcs-rust-fs v0.1.0 only supports if_generation_match=0 (create mode); "
            f"got if_generation_match={if_generation_match!r}"
        )
    if mode == "create" or if_generation_match == 0:
        return "create"
    return "overwrite"


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, str] | None:
    if not metadata:
        return None
    return {str(k): str(v) for k, v in metadata.items() if v is not None}


def _finalize_info_dict(fs: GCSFileSystem, raw: dict[str, Any]) -> dict[str, Any]:
    result = dict(raw)
    if "updated" in result and isinstance(result["updated"], str):
        result["mtime"] = fs._parse_timestamp(result["updated"])
    if "timeCreated" in result and isinstance(result["timeCreated"], str):
        result["ctime"] = fs._parse_timestamp(result["timeCreated"])
    return result


async def rust_info(
    fs: GCSFileSystem, path: str, generation: str | int | None = None
) -> dict[str, Any]:
    """Fetch object or prefix metadata via ``gcs-rust-fs`` ``StorageControl`` gRPC."""
    bridge = _require_rust_bridge()
    gen_str = str(generation) if generation is not None else None
    raw = await bridge.info_async(
        path,
        generation=gen_str,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )
    return _finalize_info_dict(fs, raw)


def rust_info_sync(
    fs: GCSFileSystem, path: str, generation: str | int | None = None
) -> dict[str, Any]:
    """Synchronous fast path for ``info`` without hopping through fsspec's asyncio loop thread."""
    bridge = _require_rust_bridge()
    gen_str = str(generation) if generation is not None else None
    raw = bridge.info(
        path,
        generation=gen_str,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )
    return _finalize_info_dict(fs, raw)


async def rust_cat_file(
    fs: GCSFileSystem,
    path: str,
    start: int | None = None,
    end: int | None = None,
    generation: str | int | None = None,
) -> bytes:
    """Read an entire object or byte sub-range via ``gcs-rust-fs``."""
    bridge = _require_rust_bridge()
    gen_str = str(generation) if generation is not None else None
    return await bridge.cat_file_async(
        path,
        start=start,
        end=end,
        generation=gen_str,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )


def rust_cat_file_sync(
    fs: GCSFileSystem,
    path: str,
    start: int | None = None,
    end: int | None = None,
    generation: str | int | None = None,
) -> bytes:
    """Synchronous fast path for ``cat_file`` without hopping through fsspec's asyncio loop thread."""
    bridge = _require_rust_bridge()
    gen_str = str(generation) if generation is not None else None
    return bridge.cat_file(
        path,
        start=start,
        end=end,
        generation=gen_str,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )


def _validate_and_build_pipe_response(
    payload: bytes, consistency: str | None
) -> dict[str, Any]:
    synthetic_resp: dict[str, Any] = {
        "size": len(payload),
        "generation": None,
    }
    if consistency in ("size", "md5"):
        checker = get_consistency_checker(consistency)
        checker.update(payload)
        if consistency == "md5":
            synthetic_resp["md5Hash"] = base64.b64encode(md5(payload).digest()).decode()
        checker.validate_json_response(synthetic_resp)
    return synthetic_resp


async def rust_pipe_file(
    fs: GCSFileSystem,
    path: str,
    data: bytes | bytearray | memoryview,
    content_type: str = "application/octet-stream",
    metadata: dict[str, Any] | None = None,
    consistency: str | None = None,
    mode: str = "overwrite",
    if_generation_match: int | None = None,
    block_size: int | None = None,
) -> dict[str, Any]:
    """Write ``data`` in one shot via ``gcs-rust-fs`` ``pipe_file``."""
    bridge = _require_rust_bridge()
    effective_mode = _resolve_rust_write_mode(mode, if_generation_match)
    payload = data if isinstance(data, bytes) else bytes(data)
    await bridge.pipe_file_async(
        path,
        payload,
        content_type=content_type,
        metadata=_normalize_metadata(metadata),
        mode=effective_mode,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )
    return _validate_and_build_pipe_response(payload, consistency)


def rust_pipe_file_sync(
    fs: GCSFileSystem,
    path: str,
    data: bytes | bytearray | memoryview,
    content_type: str = "application/octet-stream",
    metadata: dict[str, Any] | None = None,
    consistency: str | None = None,
    mode: str = "overwrite",
    if_generation_match: int | None = None,
    block_size: int | None = None,
) -> dict[str, Any]:
    """Synchronous fast path for ``pipe_file`` without hopping through fsspec's asyncio loop thread."""
    bridge = _require_rust_bridge()
    effective_mode = _resolve_rust_write_mode(mode, if_generation_match)
    payload = data if isinstance(data, bytes) else bytes(data)
    bridge.pipe_file(
        path,
        payload,
        content_type=content_type,
        metadata=_normalize_metadata(metadata),
        mode=effective_mode,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )
    return _validate_and_build_pipe_response(payload, consistency)


def rust_open_file(
    fs: GCSFileSystem,
    path: str,
    mode: str = "rb",
    content_type: str | None = None,
    metadata: dict[str, Any] | None = None,
    block_size: int | None = None,
    generation: str | int | None = None,
    if_generation_match: int | None = None,
):
    """Open a streaming ``RustGcsFile`` handle backed by ``gcs_rust_fs::GcsFile``."""
    bridge = _require_rust_bridge()
    if "w" in mode or "x" in mode:
        effective_write_mode = _resolve_rust_write_mode(
            "create" if "x" in mode else "overwrite", if_generation_match
        )
        open_mode = "xb" if effective_write_mode == "create" else "wb"
    else:
        open_mode = "rb"
    gen_str = str(generation) if generation is not None else None
    return bridge.RustGcsFile.open_sync(
        path,
        mode=open_mode,
        content_type=content_type,
        metadata=_normalize_metadata(metadata),
        block_size=block_size,
        generation=gen_str,
        transport=fs.rust_transport,
        endpoint=fs._endpoint,
    )
