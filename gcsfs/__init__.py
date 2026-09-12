import logging

try:
    from ._version import __version__  # noqa: F401
except ImportError:
    try:
        from importlib.metadata import PackageNotFoundError, version

        __version__ = version("gcsfs")
    except (ImportError, PackageNotFoundError):
        __version__ = "unknown"

logger = logging.getLogger(__name__)

from .core import GCSFileSystem  # noqa: E402
from .mapping import GCSMap  # noqa: E402

__all__ = ["GCSFileSystem", "GCSMap"]
