"""Unit and integration tests for the ``gcs-rust-fs`` PyO3 PoC backend in ``gcsfs``."""

from __future__ import annotations

from datetime import datetime
import http.server
import json
import os
import socket
import threading
import uuid

import pytest
import requests

import gcsfs

pytest.importorskip("gcs_rust_fs_py")


class _FakeGceMetadataHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if "universe-domain" in self.path:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"googleapis.com")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {
                        "access_token": "poc-test-token",
                        "expires_in": 3600,
                        "token_type": "Bearer",
                    }
                ).encode()
            )

    def log_message(self, *args):
        pass


def _pipe_socket(src: socket.socket, dst: socket.socket):
    try:
        while data := src.recv(65536):
            dst.sendall(data)
    except Exception:
        pass
    finally:
        for s in (src, dst):
            try:
                s.close()
            except Exception:
                pass


@pytest.fixture(scope="module")
def rust_testbench_endpoint():
    """Provide either a live GCS bucket (when GCSFS_TEST_BUCKET is set) or storage-testbench."""
    live_bucket = os.environ.get("GCSFS_TEST_BUCKET")
    if live_bucket:
        yield None, live_bucket
        return

    http_port = int(os.environ.get("GCSFS_TESTBENCH_HTTP_PORT", "9000"))
    grpc_port = int(os.environ.get("GCSFS_TESTBENCH_GRPC_PORT", "9001"))
    try:
        resp = requests.get(
            f"http://127.0.0.1:{http_port}/start_grpc?port={grpc_port}", timeout=2.0
        )
        resp.raise_for_status()
    except Exception as exc:
        pytest.skip(f"storage-testbench not reachable on 127.0.0.1:{http_port}: {exc}")

    meta_srv = http.server.HTTPServer(("127.0.0.1", 0), _FakeGceMetadataHandler)
    threading.Thread(target=meta_srv.serve_forever, daemon=True).start()

    old_home = os.environ.get("HOME")
    old_meta = os.environ.get("GCE_METADATA_HOST")
    os.environ["HOME"] = "/tmp"
    os.environ["GCE_METADATA_HOST"] = f"127.0.0.1:{meta_srv.server_port}"

    mux_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mux_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    mux_sock.bind(("127.0.0.1", 0))
    mux_sock.listen(128)
    mux_port = mux_sock.getsockname()[1]

    def handle_conn(client_sock: socket.socket):
        try:
            first = client_sock.recv(24, socket.MSG_PEEK)
            is_h2 = first.startswith(b"PRI * HTTP/2")
            target_port = grpc_port if is_h2 else http_port
            upstream = socket.create_connection(("127.0.0.1", target_port))
            if not is_h2:
                host_replacement = f"host: 127.0.0.1:{mux_port}\r\n".encode()
                host_cap_replacement = f"Host: 127.0.0.1:{mux_port}\r\n".encode()

                def pipe_http_req(s: socket.socket, d: socket.socket):
                    try:
                        while data := s.recv(65536):
                            data = data.replace(
                                b"host: 127.0.0.1\r\n", host_replacement
                            ).replace(b"Host: 127.0.0.1\r\n", host_cap_replacement)
                            d.sendall(data)
                    except Exception:
                        pass
                    finally:
                        for sock in (s, d):
                            try:
                                sock.close()
                            except Exception:
                                pass

                threading.Thread(
                    target=pipe_http_req, args=(client_sock, upstream), daemon=True
                ).start()
            else:
                threading.Thread(
                    target=_pipe_socket, args=(client_sock, upstream), daemon=True
                ).start()
            threading.Thread(
                target=_pipe_socket, args=(upstream, client_sock), daemon=True
            ).start()
        except Exception:
            client_sock.close()

    def accept_loop():
        while True:
            try:
                client, _ = mux_sock.accept()
            except OSError:
                break
            threading.Thread(target=handle_conn, args=(client,), daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()

    bucket = f"poc-bucket-{uuid.uuid4().hex[:8]}"
    requests.post(
        f"http://127.0.0.1:{http_port}/storage/v1/b?project=test-project",
        json={"name": bucket},
        timeout=5.0,
    )

    yield f"http://127.0.0.1:{mux_port}", bucket

    mux_sock.close()
    meta_srv.shutdown()
    if old_home is not None:
        os.environ["HOME"] = old_home
    if old_meta is not None:
        os.environ["GCE_METADATA_HOST"] = old_meta
    else:
        os.environ.pop("GCE_METADATA_HOST", None)


@pytest.mark.parametrize("transport", ["grpc", "http"])
def test_pipe_and_info_file_metadata(rust_testbench_endpoint, transport):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport=transport,
    )
    path = f"{bucket}/poc-rust-fs/info-{transport}-{uuid.uuid4().hex[:6]}.txt"
    payload = b"hello from gcs-rust-fs"

    # Act
    fs.pipe_file(
        path,
        payload,
        content_type="text/plain",
        metadata={"env": "poc", "transport": transport},
    )
    info = fs.info(path)

    # Assert
    assert info["name"] == path
    assert info["bucket"] == bucket
    assert info["type"] == "file"
    assert info["size"] == len(payload)
    assert info["contentType"] == "text/plain"
    assert info["metadata"]["env"] == "poc"
    assert info["metadata"]["transport"] == transport
    assert isinstance(info["mtime"], datetime)
    assert isinstance(info["ctime"], datetime)
    assert info["generation"] is not None


def test_info_directory_prefix(rust_testbench_endpoint):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport="grpc",
    )
    dir_path = f"{bucket}/poc-rust-fs/nested-dir-{uuid.uuid4().hex[:6]}"
    child_path = f"{dir_path}/child.bin"
    fs.pipe_file(child_path, b"nested-payload")

    # Act
    info = fs.info(dir_path)

    # Assert
    assert info["name"] == dir_path
    assert info["bucket"] == bucket
    assert info["type"] == "directory"
    assert info["size"] == 0
    assert info["storageClass"] == "DIRECTORY"


def test_info_nonexistent_raises_file_not_found(rust_testbench_endpoint):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport="grpc",
    )
    missing_path = f"{bucket}/poc-rust-fs/does-not-exist-{uuid.uuid4().hex}.bin"

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        fs.info(missing_path)


@pytest.mark.parametrize("transport", ["grpc", "http"])
@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (None, None, b"0123456789abcdefghij"),
        (0, 10, b"0123456789"),
        (10, 20, b"abcdefghij"),
        (5, None, b"56789abcdefghij"),
        (-6, None, b"efghij"),
        (-10, -4, b"abcdef"),
        (8, 8, b""),
        (12, 5, b""),
    ],
)
def test_cat_file_ranges(rust_testbench_endpoint, transport, start, end, expected):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport=transport,
    )
    path = f"{bucket}/poc-rust-fs/cat-{transport}.bin"
    fs.pipe_file(path, b"0123456789abcdefghij")

    # Act
    result = fs.cat_file(path, start=start, end=end)

    # Assert
    assert result == expected


def test_pipe_file_create_mode_conflict_raises_file_exists(rust_testbench_endpoint):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport="grpc",
    )
    path = f"{bucket}/poc-rust-fs/exclusive-{uuid.uuid4().hex[:6]}.bin"
    fs.pipe_file(path, b"initial", mode="create")

    # Act & Assert
    with pytest.raises(FileExistsError):
        fs.pipe_file(path, b"second", mode="create")


@pytest.mark.parametrize(
    ("payload_size", "block_size"),
    [
        (1024, 256 * 1024),  # Single-shot _simple_upload path (< block_size)
        (600 * 1024, 256 * 1024),  # Multi-chunk RustGcsFile streaming path (>= block_size)
    ],
)
def test_open_write_and_read_file_handle(
    rust_testbench_endpoint, payload_size, block_size
):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport="grpc",
    )
    path = f"{bucket}/poc-rust-fs/handle-{payload_size}-{uuid.uuid4().hex[:6]}.bin"
    payload = (b"RustGcsFsPoC-" * ((payload_size // 13) + 1))[:payload_size]

    # Act
    with fs.open(path, "wb", block_size=block_size) as writer:
        writer.write(payload)
    with fs.open(path, "rb", block_size=block_size, cache_type="readahead") as reader:
        first_slice = reader.read(128)
        rest_slice = reader.read()

    # Assert
    assert first_slice + rest_slice == payload
    assert fs.info(path)["size"] == payload_size


def test_open_seek_tell_and_zero_fsspec_loop(rust_testbench_endpoint):
    # Arrange
    endpoint, bucket = rust_testbench_endpoint
    fs = gcsfs.GCSFileSystem(
        project="test-project" if endpoint else None,
        token="anon" if endpoint else "google_default",
        endpoint_url=endpoint,
        use_rust_fs=True,
        rust_transport="http",
        cache_timeout=0,
    )
    path = f"{bucket}/poc-rust-fs/seek-{uuid.uuid4().hex[:6]}.bin"
    payload = bytes(range(256)) * 4096  # 1 MiB

    # Act
    with fs.open(path, "wb") as writer:
        writer.write(payload[:524288])
        writer.write(payload[524288:])

    info = fs.info(path)
    with fs.open(path, "rb") as reader:
        assert reader.cache_type == "none"
        assert reader.size == len(payload)
        assert reader.tell() == 0
        head = reader.read(64)
        assert reader.tell() == 64
        pos = reader.seek(524288)
        assert pos == 524288
        assert reader.tell() == 524288
        mid = reader.read(128)
        assert reader.tell() == 524288 + 128
        end_pos = reader.seek(-32, 2)
        assert end_pos == len(payload) - 32
        tail = reader.read()

    # Assert
    assert info["size"] == len(payload)
    assert head == payload[:64]
    assert mid == payload[524288 : 524288 + 128]
    assert tail == payload[-32:]
    assert fs._loop is None


def test_uds_sidecar_scenarios_3_and_4(rust_testbench_endpoint, tmp_path):
    # Arrange
    import subprocess
    import time

    sidecar_bin = "/home/princer_google_com/dev/gcs-rust-fs/target/release/gcsfs-sidecar"
    if not os.path.exists(sidecar_bin):
        pytest.skip(f"gcsfs-sidecar binary not found at {sidecar_bin}")

    endpoint, bucket = rust_testbench_endpoint
    sock_path = str(tmp_path / "test-gcsfs-sidecar.sock")
    env = os.environ.copy()
    if endpoint:
        env["STORAGE_EMULATOR_HOST"] = endpoint
    proc = subprocess.Popen(
        [
            sidecar_bin,
            "--uds",
            sock_path,
            "--transport",
            "http",
            "--readahead",
            "--http-pool-size",
            "4",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10.0
        while not os.path.exists(sock_path):
            if proc.poll() is not None:
                stderr = proc.stderr.read().decode("utf-8", errors="replace")
                pytest.fail(f"gcsfs-sidecar exited early: {stderr}")
            if time.monotonic() > deadline:
                pytest.fail("Timed out waiting for gcsfs-sidecar UDS socket")
            time.sleep(0.05)

        fs_s3 = gcsfs.GCSFileSystem(
            project="test-project" if endpoint else None,
            token="anon" if endpoint else "google_default",
            endpoint_url=endpoint,
            use_uds_grpc=True,
            uds_socket_path=sock_path,
            cache_timeout=0,
        )
        fs_s4 = gcsfs.GCSFileSystem(
            project="test-project" if endpoint else None,
            token="anon" if endpoint else "google_default",
            endpoint_url=endpoint,
            use_rust_fs=True,
            rust_uds_path=sock_path,
            cache_timeout=0,
        )

        for label, fs in [("scenario3_grpc_aio", fs_s3), ("scenario4_remotefs", fs_s4)]:
            path = f"{bucket}/poc-rust-fs/{label}-{uuid.uuid4().hex[:6]}.bin"
            payload = (b"UdsSidecarTest-" * 70000)[: 1024 * 1024]  # 1 MiB
            fs.pipe_file(path, payload, content_type="application/octet-stream")
            info = fs.info(path)
            assert info["size"] == len(payload)
            assert fs.cat_file(path, start=100, end=250) == payload[100:250]

            with fs.open(path, "rb") as f:
                assert f.read(256) == payload[:256]
                assert f.read(512) == payload[256:768]
                f.seek(500000)
                assert f.read(1024) == payload[500000:501024]
                assert f.read(1024) == payload[501024:502048]

            stream_path = f"{bucket}/poc-rust-fs/{label}-stream-{uuid.uuid4().hex[:6]}.bin"
            with fs.open(stream_path, "wb") as wf:
                wf.write(payload[:524288])
                wf.write(payload[524288:])
            assert fs.cat_file(stream_path) == payload
            assert fs._loop is None
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

