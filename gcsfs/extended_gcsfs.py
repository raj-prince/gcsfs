"""Backward-compatibility module for ExtendedGcsFileSystem.

ExtendedGcsFileSystem has been unified into GCSFileSystem via the Storage
Bucket Driver architecture. GCSFileSystem now natively supports Flat,
Hierarchical Namespace (HNS), and Zonal buckets.
"""

from __future__ import annotations

from gcsfs.core import (
    GCSFile,
    GCSFileSystem,
    initiate_upload,
    simple_upload,
    upload_chunk,
)
from gcsfs.drivers.base import BucketType
from gcsfs.drivers.zonal import (
    _get_mrd_from_pool_or_mrd,
    _get_mrd_size,
)
from gcsfs.zonal_file import ZonalFile

gcs_file_types = (GCSFile, ZonalFile)


class ExtendedGcsFileSystem(GCSFileSystem):
    """Deprecated: Use GCSFileSystem directly.

    All HNS and Zonal bucket capabilities are now unified into GCSFileSystem.
    """

    pass


__all__ = [
    "BucketType",
    "ExtendedGcsFileSystem",
    "gcs_file_types",
    "initiate_upload",
    "simple_upload",
    "upload_chunk",
    "_get_mrd_from_pool_or_mrd",
    "_get_mrd_size",
]
