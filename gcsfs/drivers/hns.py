"""Hierarchical Namespace (HNS) bucket driver using StorageControl gRPC API."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict
from urllib.parse import quote  # noqa: F401

from google.api_core import exceptions as api_exceptions
from google.cloud import storage_control_v2

from gcsfs.drivers.base import BucketType
from gcsfs.drivers.flat import FlatBucketDriver

if TYPE_CHECKING:
    from gcsfs.core import GCSFileSystem  # noqa: F401

logger = logging.getLogger("gcsfs")
STORAGE_CONTROL_RPC_TIMEOUT = 30.0


class HnsBucketDriver(FlatBucketDriver):
    """Driver strategy for Hierarchical Namespace (HNS) buckets.

    Uses StorageControl v2 gRPC client for directory/folder operations,
    and inherits standard flat data I/O for file operations.
    """

    @property
    def bucket_type(self) -> BucketType:
        return BucketType.HIERARCHICAL

    async def mkdir(
        self,
        path: str,
        create_parents: bool = True,
        exist_ok: bool = True,
        **kwargs: Any,
    ) -> None:
        path = self.fs._strip_protocol(path)
        bucket, key, _ = self.fs.split_path(path)
        if not key:
            await self.fs._mkdir_flat(path, **kwargs)
            return

        logger.debug(f"Using HNS-aware mkdir for '{path}'.")
        try:
            info = await self.fs._info(path)
            if info["type"] != "directory":
                raise FileExistsError(f"A file already exists at the path: {path}")
            if not exist_ok:
                raise FileExistsError(f"Directory already exists: {path}")
            return
        except FileNotFoundError:
            pass

        parent = f"projects/_/buckets/{bucket}"
        folder_id = key.rstrip("/") + "/"
        request = storage_control_v2.CreateFolderRequest(
            parent=parent,
            folder_id=folder_id,
            recursive=create_parents,
            request_id=str(uuid.uuid4()),
        )
        try:
            client = await self.fs._get_control_plane_client()
            await client.create_folder(
                request=request,
                retry=self.fs._get_retry_config(),
                timeout=STORAGE_CONTROL_RPC_TIMEOUT,
            )
            self.fs._cache_add_entry(
                self.fs._parent(path),
                self.fs._directory_cache_entry(path, key.rstrip("/")),
            )
        except api_exceptions.Conflict as e:
            if not exist_ok:
                raise FileExistsError(f"Directory already exists: {path}") from e
        except api_exceptions.FailedPrecondition as e:
            raise FileNotFoundError(
                f"mkdir for '{path}' failed due to a precondition error: {e}"
            ) from e

    async def rmdir(self, path: str) -> None:
        path = self.fs._strip_protocol(path)
        bucket, key, _ = self.fs.split_path(path)
        if not key:
            await self.fs._rmdir_flat(path)
            return

        try:
            placeholder_path = f"{path.rstrip('/')}/"
            await self.fs._rm_file(placeholder_path)
        except FileNotFoundError:
            pass

        folder_name = f"projects/_/buckets/{bucket}/folders/{key.rstrip('/')}"
        request = storage_control_v2.DeleteFolderRequest(
            name=folder_name,
            request_id=str(uuid.uuid4()),
        )
        try:
            client = await self.fs._get_control_plane_client()
            await client.delete_folder(
                request=request,
                retry=self.fs._get_retry_config(),
                timeout=STORAGE_CONTROL_RPC_TIMEOUT,
            )
            self.fs.invalidate_cache(self.fs._parent(path))
            self.fs.invalidate_cache(path)
        except api_exceptions.NotFound as e:
            raise FileNotFoundError(f"Directory not found: {path}") from e
        except api_exceptions.FailedPrecondition as e:
            raise OSError(f"Directory not empty: {path}") from e

    async def get_directory_info(
        self,
        path: str,
        bucket: str,
        key: str,
        generation: Any,
    ) -> Dict[str, Any]:
        try:
            folder_id = key.rstrip("/")
            folder_resource_name = f"projects/_/buckets/{bucket}/folders/{folder_id}"
            request = storage_control_v2.GetFolderRequest(
                name=folder_resource_name, request_id=str(uuid.uuid4())
            )
            client = await self.fs._get_control_plane_client()
            response = await client.get_folder(
                request=request,
                retry=self.fs._get_retry_config(),
                timeout=STORAGE_CONTROL_RPC_TIMEOUT,
            )
            return {
                "bucket": bucket,
                "name": path,
                "size": 0,
                "storageClass": "DIRECTORY",
                "type": "directory",
                "ctime": response.create_time,
                "mtime": response.update_time,
                "metageneration": response.metageneration,
            }
        except api_exceptions.NotFound:
            raise FileNotFoundError(path)
        except Exception as e:
            logger.error(f"Error fetching folder metadata for {path}: {e}")
            raise e

    async def rename_folder(self, bucket: str, key1: str, key2: str) -> None:
        source_folder_name = f"projects/_/buckets/{bucket}/folders/{key1}"
        destination_folder_id = key2 or key1.rstrip("/").split("/")[-1]
        request = storage_control_v2.RenameFolderRequest(
            name=source_folder_name,
            destination_folder_id=destination_folder_id,
            request_id=str(uuid.uuid4()),
        )
        client = await self.fs._get_control_plane_client()
        operation = await client.rename_folder(
            request=request,
            retry=self.fs._get_retry_config(),
            timeout=STORAGE_CONTROL_RPC_TIMEOUT,
        )
        await operation.result()
