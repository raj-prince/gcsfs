import logging
import pytest
import gcsfs

try:
    import gcs_file_spec

    RUST_AVAILABLE = True
except ImportError:
    gcs_file_spec = None
    RUST_AVAILABLE = False

pytestmark = pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust backend extension not available")

SAMPLE_FILE = "princer-bucket/pytorch_gcsfs_checkpoint_architecture.md"


def test_gcsfs_default_grpc_backend(caplog):
    """Test GCSFileSystem defaults to gRPC backend without explicit parameter."""
    caplog.set_level(logging.INFO, logger="gcsfs")
    fs = gcsfs.GCSFileSystem()
    assert fs.read_backend == "grpc"
    assert fs.rust_transport == "grpc"

    meta = fs.info(SAMPLE_FILE)
    assert meta["name"] == SAMPLE_FILE
    assert meta["size"] > 0
    assert any("[Rust Backend] stat executing with rust backend" in record.message for record in caplog.records)

    caplog.clear()
    data = fs.cat_file(SAMPLE_FILE, start=0, end=20)
    assert data == b"# PyTorch Checkpoint"
    assert any("[Rust Backend] cat_file executing with rust backend" in record.message for record in caplog.records)


def test_gcsfs_explicit_grpc_backend(caplog):
    """Test GCSFileSystem with explicit read_backend='grpc'."""
    caplog.set_level(logging.INFO, logger="gcsfs")
    fs = gcsfs.GCSFileSystem(read_backend="grpc")

    meta = fs.info(SAMPLE_FILE)
    assert meta["name"] == SAMPLE_FILE
    assert meta["size"] > 0

    data = fs.cat_file(SAMPLE_FILE, start=0, end=20)
    assert data == b"# PyTorch Checkpoint"


def test_gcsfs_cat_file_empty_range():
    """Verify empty range returns b'' without remote call error."""
    fs = gcsfs.GCSFileSystem()
    assert fs.cat_file(SAMPLE_FILE, start=10, end=10) == b""
    assert fs.cat_file(SAMPLE_FILE, start=15, end=10) == b""


def test_gcsfs_file_not_found():
    """Verify FileNotFoundError is raised for nonexistent paths."""
    fs = gcsfs.GCSFileSystem()
    with pytest.raises(FileNotFoundError):
        fs.info("princer-bucket/nonexistent_file_xyz_123.txt")
