"""Zonal (Rapid) bucket driver using high-performance gRPC and AsyncMultiRangeDownloader."""

from __future__ import annotations

import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiohttp
from fsspec.callbacks import NoOpCallback
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

from gcsfs import zb_hns_utils
from gcsfs.drivers.base import BucketType
from gcsfs.drivers.hns import HnsBucketDriver
from gcsfs.zb_hns_utils import MRDPool

if TYPE_CHECKING:
    from gcsfs.core import GCSFileSystem  # noqa: F401
    from gcsfs.zonal_file import ZonalFile

logger = logging.getLogger("gcsfs")


@contextlib.asynccontextmanager
async def _get_mrd_from_pool_or_mrd(mrd_or_pool: Any):
    """Yield an AsyncMultiRangeDownloader whether single instance or MRDPool."""
    if isinstance(mrd_or_pool, MRDPool):
        async with mrd_or_pool.get_mrd() as m:
            yield m
    elif isinstance(mrd_or_pool, AsyncMultiRangeDownloader):
        yield mrd_or_pool
    else:
        raise TypeError(
            f"Expected MRDPool or AsyncMultiRangeDownloader, got {type(mrd_or_pool)}"
        )


async def _get_mrd_size(mrd_or_pool: Any) -> Optional[int]:
    """Extract persisted_size from pool or single MRD."""
    if mrd_or_pool is None:
        return None
    async with _get_mrd_from_pool_or_mrd(mrd_or_pool) as m:
        return m.persisted_size


class ZonalBucketDriver(HnsBucketDriver):
    """Driver strategy for Zonal (Rapid) buckets.

    Inherits HNS folder management from HnsBucketDriver and implements
    high-performance gRPC streaming reads (ZonalFile/MRDPool) and writes (AAOW).
    """

    @property
    def bucket_type(self) -> BucketType:
        return BucketType.ZONAL_HIERARCHICAL

    def open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> ZonalFile:
        from gcsfs.zonal_file import ZonalFile

        return ZonalFile(self.fs, path, mode=mode, **kwargs)

    async def cat_file(
        self,
        path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        mrd: Any = None,
        **kwargs: Any,
    ) -> bytes:
        concurrency = kwargs.pop("concurrency", 1)
        pool_created_here = False
        cache_type, cache_source = self.fs._resolve_cache_config(kwargs)

        if mrd is None:
            bucket, object_name, generation = self.fs.split_path(path)
            mrd = await self.fs._mrd_pool_cache.get(
                bucket,
                object_name,
                generation,
                pool_size=concurrency,
                cache_type=cache_type,
                cache_source=cache_source,
            )
            pool_created_here = True

        try:
            file_size = await _get_mrd_size(mrd)
            if file_size is None:
                logger.warning(
                    f"AsyncMultiRangeDownloader (MRD) for {path} has no 'persisted_size'. "
                    "Falling back to _info() to get the file size. "
                    "This may result in incorrect behavior for unfinalized objects."
                )
                file_size = (await self.fs._info(path))["size"]

            offset, length = await self.fs._process_limits_to_offset_and_length(
                path, start, end, file_size
            )

            if length == 0:
                return b""

            return await self.fs._concurrent_mrd_fetch(
                offset,
                length,
                concurrency,
                mrd,
            )
        finally:
            if pool_created_here:
                await mrd.close()

    async def upload_chunk(
        self,
        location: Any,
        data: bytes,
        offset: int,
        size: int,
        content_type: str,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(location, (str, bytes)):
            return await super().upload_chunk(
                location, data, offset, size, content_type
            )

        if not isinstance(location, AsyncAppendableObjectWriter):
            raise TypeError(
                "upload_chunk for Zonal buckets expects an AsyncAppendableObjectWriter instance."
            )

        if not location._is_stream_open:
            raise ValueError("Writer is closed. Please initiate a new upload.")

        try:
            await location.append(data)
        except Exception as e:
            logger.error(
                f"Error uploading chunk at offset {location.offset}: {e}. Closing stream."
            )
            await zb_hns_utils.close_aaow(location, finalize_on_close=False)
            raise

        if (location.offset or 0) >= size:
            logger.debug(
                "Uploaded data is equal or greater than size. Finalizing upload."
            )
            await zb_hns_utils.close_aaow(location, finalize_on_close=True)
        return None

    async def initiate_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        fixed_key_metadata: Optional[Dict[str, Any]] = None,
        mode: str = "overwrite",
        kms_key_name: Optional[str] = None,
    ) -> Any:
        if (
            metadata
            or fixed_key_metadata
            or kms_key_name
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadata, fixed_key_metadata, "
                "or kms_key_name during upload. These parameters will be ignored."
            )
        await self.fs._get_grpc_client()
        return await zb_hns_utils.init_aaow(self.fs.grpc_client, bucket, key)

    async def simple_upload(
        self,
        bucket: str,
        key: str,
        datain: bytes,
        metadatain: Optional[Dict[str, Any]] = None,
        consistency: Optional[str] = None,
        content_type: str = "application/octet-stream",
        fixed_key_metadata: Optional[Dict[str, Any]] = None,
        mode: str = "overwrite",
        kms_key_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        if (
            metadatain
            or fixed_key_metadata
            or kms_key_name
            or consistency
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadatain, fixed_key_metadata, "
                "consistency or kms_key_name during upload. These parameters will be ignored."
            )
        await self.fs._get_grpc_client()
        writer = await zb_hns_utils.init_aaow(self.fs.grpc_client, bucket, key)
        try:
            await writer.append(datain)
        finally:
            default_finalize = getattr(self.fs, "finalize_on_close", False)
            finalize_on_close = kwargs.get("finalize_on_close", default_finalize)
            await zb_hns_utils.close_aaow(writer, finalize_on_close=finalize_on_close)

    async def put_file(
        self,
        lpath: str,
        rpath: str,
        metadata: Optional[Dict[str, Any]] = None,
        consistency: Optional[str] = None,
        content_type: Optional[str] = None,
        chunksize: int = 50 * 2**20,
        callback: Any = None,
        fixed_key_metadata: Optional[Dict[str, Any]] = None,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        bucket, key, generation = self.fs.split_path(rpath)
        if os.path.isdir(lpath):
            return

        if generation:
            raise ValueError("Cannot write to specific object generation")

        if (
            metadata
            or fixed_key_metadata
            or consistency
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadata, "
                "fixed_key_metadata or consistency during upload. "
                "These parameters will be ignored."
            )
        await self.fs._get_grpc_client()
        writer = await zb_hns_utils.init_aaow(self.fs.grpc_client, bucket, key)

        try:
            with open(lpath, "rb") as f:
                await writer.append_from_file(f, block_size=chunksize)
        finally:
            finalize_on_close = kwargs.get(
                "finalize_on_close", getattr(self.fs, "finalize_on_close", False)
            )
            await zb_hns_utils.close_aaow(writer, finalize_on_close=finalize_on_close)

        await self.fs._write_file_cache_update(rpath)

    async def pipe_file(
        self,
        path: str,
        data: bytes,
        metadata: Optional[Dict[str, Any]] = None,
        consistency: Optional[str] = None,
        content_type: str = "application/octet-stream",
        fixed_key_metadata: Optional[Dict[str, Any]] = None,
        chunksize: int = 50 * 2**20,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        bucket, key, generation = self.fs.split_path(path)
        if (
            metadata
            or fixed_key_metadata
            or (content_type and content_type != "application/octet-stream")
        ):
            logger.warning(
                "Zonal buckets do not support content_type, metadata or "
                "fixed_key_metadata during upload. These parameters will be ignored."
            )
        await self.fs._get_grpc_client()
        writer = await zb_hns_utils.init_aaow(self.fs.grpc_client, bucket, key)
        try:
            await writer.append(data)
        finally:
            finalize_on_close = kwargs.get(
                "finalize_on_close", getattr(self.fs, "finalize_on_close", False)
            )
            await zb_hns_utils.close_aaow(writer, finalize_on_close=finalize_on_close)

        await self.fs._write_file_cache_update(path)

    async def get_file_request(
        self,
        rpath: str,
        lpath: str,
        *args: Any,
        headers: Optional[Dict[str, Any]] = None,
        callback: Any = None,
        **kwargs: Any,
    ) -> None:
        if os.path.isdir(lpath):
            return

        bucket, key, path_generation = self.fs.split_path(rpath)
        generation = path_generation or kwargs.get("generation")
        callback = callback or NoOpCallback()
        cache_type, cache_source = self.fs._resolve_cache_config(kwargs)

        mrd_pool = await self.fs._mrd_pool_cache.get(
            bucket,
            key,
            generation,
            pool_size=1,
            cache_type=cache_type,
            cache_source=cache_source,
        )
        try:
            async with mrd_pool.get_mrd() as mrd:
                size = mrd.persisted_size
                if size is None:
                    logger.warning(
                        f"AsyncMultiRangeDownloader (MRD) for {rpath} has no 'persisted_size'. "
                        "Falling back to _info() to get the file size. "
                        "This may result in incorrect behavior for unfinalized objects."
                    )
                    size = (await self.fs._info(rpath, **kwargs)).get("size", 0)

                callback.set_size(size)
                lparent = os.path.dirname(lpath) or os.curdir
                os.makedirs(lparent, exist_ok=True)

                chunksize = kwargs.get("chunksize", 4096 * 32)
                offset = 0

                with open(lpath, "wb") as f2:
                    while True:
                        if offset >= size:
                            break

                        data = await zb_hns_utils.download_range(
                            offset=offset, length=chunksize, mrd=mrd
                        )
                        if not data:
                            break

                        f2.write(data)
                        offset += len(data)
                        callback.relative_update(len(data))

                if offset != size:
                    raise aiohttp.ClientError(
                        f"Expected {size} bytes, but only received {offset} bytes"
                    )
        except Exception as e:
            if os.path.exists(lpath):
                os.remove(lpath)
            raise e
        finally:
            await mrd_pool.close()

    async def get_file_concurrent(
        self,
        rpath: str,
        lpath: str,
        concurrency: int,
        chunk_size: int,
        max_prefetch_size: int,
        headers: Optional[Dict[str, Any]] = None,
        callback: Any = None,
        fetcher_fn: Any = None,
        **kwargs: Any,
    ) -> None:
        bucket, key, path_generation = self.fs.split_path(rpath)
        generation = path_generation or kwargs.get("generation")
        cache_type, cache_source = self.fs._resolve_cache_config(kwargs)

        mrd_pool = await self.fs._mrd_pool_cache.get(
            bucket,
            key,
            generation,
            pool_size=concurrency,
            cache_type=cache_type,
            cache_source=cache_source,
        )

        async def custom_fetcher(start, size, split_factor=1):
            return await self.cat_file(
                rpath,
                start=start,
                end=start + size,
                mrd=mrd_pool,
                concurrency=split_factor,
                **kwargs,
            )

        try:
            await self.fs._get_file_concurrent_flat(
                rpath,
                lpath,
                concurrency,
                chunk_size,
                max_prefetch_size,
                headers=headers,
                callback=callback,
                fetcher_fn=custom_fetcher,
                **kwargs,
            )
        finally:
            await mrd_pool.close()

    async def cp_file(
        self,
        path1: str,
        path2: str,
        acl: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        raise NotImplementedError(
            "Server-side copy involving Zonal buckets is not supported. "
            "Zonal objects do not support rewrite."
        )

    async def merge(
        self,
        path: str,
        paths: List[str],
        acl: Optional[str] = None,
    ) -> Any:
        raise NotImplementedError(
            "Server-side compose/merge is not supported for Zonal buckets."
        )
