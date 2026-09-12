"""Standard flat-namespace bucket driver using Google Cloud Storage JSON REST API."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.parse import quote

from gcsfs.checkers import get_consistency_checker
from gcsfs.drivers.base import BaseBucketDriver, BucketType

if TYPE_CHECKING:
    from gcsfs.core import GCSFile, GCSFileSystem  # noqa: F401


class FlatBucketDriver(BaseBucketDriver):
    """Driver strategy for standard flat-namespace Google Cloud Storage buckets."""

    @property
    def bucket_type(self) -> BucketType:
        return BucketType.NON_HIERARCHICAL

    async def cat_file(
        self,
        path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        **kwargs: Any,
    ) -> bytes:
        concurrency = kwargs.pop("concurrency", 1)
        if concurrency > 1:
            return await self.fs._cat_file_concurrent(
                path, start=start, end=end, concurrency=concurrency, **kwargs
            )
        return await self.fs._cat_file_sequential(path, start=start, end=end, **kwargs)

    def open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> GCSFile:
        from gcsfs.core import GCSFile

        kwargs.pop("finalize_on_close", None)
        return GCSFile(self.fs, path, mode=mode, **kwargs)

    async def mkdir(
        self,
        path: str,
        **kwargs: Any,
    ) -> None:
        await self.fs._mkdir_flat(path, **kwargs)

    async def rmdir(
        self,
        path: str,
    ) -> None:
        await self.fs._rmdir_flat(path)

    async def rm_files(
        self,
        files: List[str],
        batchsize: int = 100,
    ) -> List[Any]:
        return await self.fs._delete_files(files, batchsize=batchsize)

    async def upload_chunk(
        self,
        location: Any,
        data: bytes,
        offset: int,
        size: int,
        content_type: str,
    ) -> Optional[Dict[str, Any]]:
        from gcsfs.core import UnclosableBytesIO

        head = {}
        l = len(data)
        range_header = "bytes %i-%i/%i" % (offset, offset + l - 1, size)
        head["Content-Range"] = range_header
        head.update({"Content-Type": content_type, "Content-Length": str(l)})
        payload = (
            data
            if isinstance(data, (bytes, bytearray, memoryview))
            else UnclosableBytesIO(data)
        )
        headers, txt = await self.fs._call("POST", location, headers=head, data=payload)
        if "Range" in headers:
            end = int(headers["Range"].split("-")[1])
            shortfall = (offset + l - 1) - end
            if shortfall:
                return await self.upload_chunk(
                    location, data[-shortfall:], end + 1, size, content_type
                )
        return json.loads(txt) if txt else None

    async def initiate_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        fixed_key_metadata: Optional[Dict[str, Any]] = None,
        mode: str = "overwrite",
        kms_key_name: Optional[str] = None,
    ) -> str:
        from gcsfs.core import _convert_fixed_key_metadata

        j = {"name": key}
        if metadata:
            j["metadata"] = metadata
        kw = {"ifGenerationMatch": "0"} if mode == "create" else {}
        if kms_key_name:
            kw["kmsKeyName"] = kms_key_name
        j.update(_convert_fixed_key_metadata(fixed_key_metadata))
        headers, _ = await self.fs._call(
            method="POST",
            path=f"{self.fs._location}/upload/storage/v1/b/{quote(bucket)}/o?name={quote(key)}",
            uploadType="resumable",
            json=j,
            headers={"X-Upload-Content-Type": content_type},
            **kw,
        )
        loc = headers["Location"]
        out = loc[0] if isinstance(loc, list) else loc
        return out

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
        from gcsfs.core import UnclosableBytesIO, _convert_fixed_key_metadata

        checker = get_consistency_checker(consistency)
        path = f"{self.fs._location}/upload/storage/v1/b/{quote(bucket)}/o"
        metadata = {"name": key}
        if metadatain is not None:
            metadata["metadata"] = metadatain
        kw = {"ifGenerationMatch": "0"} if mode == "create" else {}
        if kms_key_name:
            kw["kmsKeyName"] = kms_key_name
        metadata.update(_convert_fixed_key_metadata(fixed_key_metadata))
        metadata_str = json.dumps(metadata)
        template = (
            "--==0=="
            "\nContent-Type: application/json; charset=UTF-8"
            "\n\n"
            + metadata_str
            + "\n--==0=="
            + f"\nContent-Type: {content_type}"
            + "\n\n"
        )
        data = template.encode() + datain + b"\n--==0==--"
        payload = (
            data
            if isinstance(data, (bytes, bytearray, memoryview))
            else UnclosableBytesIO(data)
        )
        await self.fs._call(
            "POST",
            path,
            uploadType="multipart",
            headers={"Content-Type": 'multipart/related; boundary="==0=="'},
            data=payload,
            json_out=True,
            **kw,
        )
        checker.update(datain)

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
        await self.fs._put_file_flat(
            lpath,
            rpath,
            metadata=metadata,
            consistency=consistency,
            content_type=content_type,
            chunksize=chunksize,
            callback=callback,
            fixed_key_metadata=fixed_key_metadata,
            mode=mode,
            **kwargs,
        )

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
        await self.fs._pipe_file_flat(
            path,
            data,
            metadata=metadata,
            consistency=consistency,
            content_type=content_type,
            fixed_key_metadata=fixed_key_metadata,
            chunksize=chunksize,
            mode=mode,
            **kwargs,
        )

    async def get_file_request(
        self,
        rpath: str,
        lpath: str,
        *args: Any,
        headers: Optional[Dict[str, Any]] = None,
        callback: Any = None,
        **kwargs: Any,
    ) -> None:
        await self.fs._get_file_request_flat(
            rpath, lpath, *args, headers=headers, callback=callback, **kwargs
        )

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
        await self.fs._get_file_concurrent_flat(
            rpath,
            lpath,
            concurrency,
            chunk_size,
            max_prefetch_size,
            headers=headers,
            callback=callback,
            fetcher_fn=fetcher_fn,
            **kwargs,
        )

    async def cp_file(
        self,
        path1: str,
        path2: str,
        acl: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        await self.fs._cp_file_flat(path1, path2, acl=acl, **kwargs)

    async def merge(
        self,
        path: str,
        paths: List[str],
        acl: Optional[str] = None,
    ) -> Any:
        return await self.fs._merge_flat(path, paths, acl=acl)

    async def get_directory_info(
        self,
        path: str,
        bucket: str,
        key: str,
        generation: Any,
    ) -> Dict[str, Any]:
        from gcsfs.core import _is_directory_marker

        out = await self.fs._list_objects(path, max_results=1)
        exact = next((o for o in out if o["name"].rstrip("/") == path), None)
        if exact and not _is_directory_marker(exact):
            return exact
        elif out:
            return {
                "bucket": bucket,
                "name": path,
                "size": 0,
                "storageClass": "DIRECTORY",
                "type": "directory",
            }
        else:
            raise FileNotFoundError(path)
