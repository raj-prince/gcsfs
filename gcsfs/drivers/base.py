"""Base abstract driver interface for Google Cloud Storage bucket types.

Defines the protocol and abstract base class for bucket-specific operations
across Flat (standard), Hierarchical Namespace (HNS), and Zonal buckets.
"""

from __future__ import annotations

import abc
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from gcsfs.core import GCSFileSystem


class BucketType(Enum):
    ZONAL_HIERARCHICAL = "ZONAL_HIERARCHICAL"
    HIERARCHICAL = "HIERARCHICAL"
    NON_HIERARCHICAL = "NON_HIERARCHICAL"
    UNKNOWN = "UNKNOWN"


class BaseBucketDriver(abc.ABC):
    """Abstract base class for bucket-level storage driver strategies."""

    def __init__(self, fs: GCSFileSystem):
        self.fs = fs

    @property
    def bucket_type(self) -> BucketType:
        """The BucketType handled by this driver."""
        raise NotImplementedError

    @property
    def is_hns(self) -> bool:
        """Whether this bucket has Hierarchical Namespace enabled."""
        return self.bucket_type in (
            BucketType.HIERARCHICAL,
            BucketType.ZONAL_HIERARCHICAL,
        )

    @property
    def is_zonal(self) -> bool:
        """Whether this bucket is a Zonal (Rapid) bucket."""
        return self.bucket_type == BucketType.ZONAL_HIERARCHICAL

    @abc.abstractmethod
    async def cat_file(
        self,
        path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        **kwargs: Any,
    ) -> bytes:
        """Fetch a file's contents as bytes."""
        raise NotImplementedError

    @abc.abstractmethod
    def open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        """Open a file for reading or writing."""
        raise NotImplementedError

    @abc.abstractmethod
    async def mkdir(
        self,
        path: str,
        **kwargs: Any,
    ) -> None:
        """Create a directory or folder."""
        raise NotImplementedError

    @abc.abstractmethod
    async def rmdir(
        self,
        path: str,
    ) -> None:
        """Remove a directory or folder."""
        raise NotImplementedError

    @abc.abstractmethod
    async def rm_files(
        self,
        files: List[str],
        batchsize: int = 100,
    ) -> List[Any]:
        """Delete a list of files in the bucket."""
        raise NotImplementedError

    @abc.abstractmethod
    async def upload_chunk(
        self,
        location: Any,
        data: bytes,
        offset: int,
        size: int,
        content_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Upload a chunk of data during a multi-part or streaming upload."""
        raise NotImplementedError

    @abc.abstractmethod
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
        """Initiate an upload session (resumable URL or appendable writer)."""
        raise NotImplementedError

    @abc.abstractmethod
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
        """Perform a single-request upload."""
        raise NotImplementedError

    @abc.abstractmethod
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
        """Upload a local file to the bucket."""
        raise NotImplementedError

    @abc.abstractmethod
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
        """Upload bytes directly to a file."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_file_request(
        self,
        rpath: str,
        lpath: str,
        *args: Any,
        headers: Optional[Dict[str, Any]] = None,
        callback: Any = None,
        **kwargs: Any,
    ) -> None:
        """Download a file from GCS to local disk via sequential request."""
        raise NotImplementedError

    @abc.abstractmethod
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
        """Download a file concurrently to local disk."""
        raise NotImplementedError

    @abc.abstractmethod
    async def cp_file(
        self,
        path1: str,
        path2: str,
        acl: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Copy a file within or between buckets."""
        raise NotImplementedError

    @abc.abstractmethod
    async def merge(
        self,
        path: str,
        paths: List[str],
        acl: Optional[str] = None,
    ) -> Any:
        """Concatenate objects within a single bucket."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_directory_info(
        self,
        path: str,
        bucket: str,
        key: str,
        generation: Any,
    ) -> Dict[str, Any]:
        """Fetch directory metadata."""
        raise NotImplementedError
