//! Thin PyO3 bridge exposing `gcs-rust-fs` (`info`, `read` / `cat_file`, and `write` / `pipe_file`)
//! to `gcsfs`.

use bytes::Bytes;
use gcs_rust_fs::{
    ByteRange, Entry, EntryKind, Error as FsError, ErrorKind, File, FileSystem, GcsFs, OpenOptions,
    ReadOptions, Transport, WriteMode, WriteOptions,
};
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIOError, PyIsADirectoryError, PyNotADirectoryError,
    PyNotImplementedError, PyPermissionError, PyTimeoutError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;

static FS_CACHE: StdMutex<Option<HashMap<(String, String), GcsFs>>> = StdMutex::new(None);

fn worker_threads() -> usize {
    std::env::var("GCSFS_RUST_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(16)
}

fn init_runtime() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads()).enable_all();
    pyo3_async_runtimes::tokio::init(builder);
}

fn to_py_err(err: FsError) -> PyErr {
    let msg = err.to_string();
    match err.kind() {
        ErrorKind::NotFound => PyFileNotFoundError::new_err(msg),
        ErrorKind::PermissionDenied | ErrorKind::Unauthenticated => PyPermissionError::new_err(msg),
        ErrorKind::IsADirectory => PyIsADirectoryError::new_err(msg),
        ErrorKind::NotADirectory => PyNotADirectoryError::new_err(msg),
        ErrorKind::AlreadyExists => PyFileExistsError::new_err(msg),
        ErrorKind::InvalidPath
        | ErrorKind::InvalidRange
        | ErrorKind::InvalidConfig
        | ErrorKind::Closed => PyValueError::new_err(msg),
        ErrorKind::Unsupported => PyNotImplementedError::new_err(msg),
        ErrorKind::Timeout => PyTimeoutError::new_err(msg),
        _ => PyIOError::new_err(msg),
    }
}

async fn get_fs(transport: Option<&str>, endpoint: Option<&str>) -> PyResult<GcsFs> {
    let trans_str = match transport.map(|t| t.to_ascii_lowercase()).as_deref() {
        Some("http") | Some("json") | Some("rest") => "http",
        _ => "grpc",
    };
    let resolved_endpoint = endpoint
        .map(|s| s.to_string())
        .or_else(|| std::env::var("STORAGE_EMULATOR_HOST").ok())
        .filter(|s| !s.trim().is_empty() && s != "https://storage.googleapis.com");
    let key = (
        trans_str.to_string(),
        resolved_endpoint.clone().unwrap_or_default(),
    );

    if let Some(existing) = FS_CACHE
        .lock()
        .expect("fs cache lock")
        .as_ref()
        .and_then(|m| m.get(&key).cloned())
    {
        return Ok(existing);
    }

    let mut builder = GcsFs::builder().from_env().map_err(to_py_err)?;
    builder = builder.transport(if trans_str == "http" {
        Transport::Http
    } else {
        Transport::Grpc
    });
    if let Some(ep) = resolved_endpoint {
        builder = builder.endpoint(ep);
    }
    let fs = builder.build().await.map_err(to_py_err)?;
    let mut guard = FS_CACHE.lock().expect("fs cache lock");
    guard
        .get_or_insert_with(HashMap::new)
        .insert(key, fs.clone());
    Ok(fs)
}

fn format_path_with_generation(path: &str, generation: Option<&str>) -> String {
    match generation {
        Some(gen) if !gen.is_empty() && !path.contains('#') => format!("{path}#{gen}"),
        _ => path.to_string(),
    }
}

fn entry_to_py_dict<'py>(py: Python<'py>, entry: &Entry) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &entry.path)?;
    dict.set_item("size", entry.size)?;
    match entry.kind {
        EntryKind::Directory => {
            dict.set_item("type", "directory")?;
            dict.set_item("storageClass", "DIRECTORY")?;
            if let Some((bucket, _)) = entry.path.split_once('/') {
                dict.set_item("bucket", bucket)?;
            } else {
                dict.set_item("bucket", &entry.path)?;
            }
        }
        EntryKind::File => {
            dict.set_item("type", "file")?;
            if let Some(ref stat) = entry.stat {
                dict.set_item("bucket", &stat.bucket)?;
                dict.set_item("generation", stat.generation.to_string())?;
                dict.set_item("metageneration", stat.metageneration.to_string())?;
                if !stat.content_type.is_empty() {
                    dict.set_item("contentType", &stat.content_type)?;
                }
                if let Some(ref ce) = stat.content_encoding {
                    dict.set_item("contentEncoding", ce)?;
                }
                if let Some(ref cc) = stat.cache_control {
                    dict.set_item("cacheControl", cc)?;
                }
                if let Some(ref cd) = stat.content_disposition {
                    dict.set_item("contentDisposition", cd)?;
                }
                if let Some(ref cl) = stat.content_language {
                    dict.set_item("contentLanguage", cl)?;
                }
                if !stat.storage_class.is_empty() {
                    dict.set_item("storageClass", &stat.storage_class)?;
                }
                if !stat.etag.is_empty() {
                    dict.set_item("etag", &stat.etag)?;
                }
                if let Some(ref crc) = stat.crc32c_base64() {
                    dict.set_item("crc32c", crc)?;
                }
                if let Some(ref md5) = stat.md5_hash {
                    dict.set_item("md5Hash", md5)?;
                }
                if let Some(ref kms) = stat.kms_key {
                    dict.set_item("kmsKeyName", kms)?;
                }
                if let Some(cc) = stat.component_count {
                    dict.set_item("componentCount", cc)?;
                }
                if let Some(ref tc) = stat.time_created {
                    dict.set_item("timeCreated", tc)?;
                }
                if let Some(ref up) = stat.updated {
                    dict.set_item("updated", up)?;
                }
                if let Some(ref ct_time) = stat.custom_time {
                    dict.set_item("customTime", ct_time)?;
                }
                if !stat.metadata.is_empty() {
                    dict.set_item("metadata", &stat.metadata)?;
                }
            }
        }
    }
    Ok(dict)
}

#[pyfunction]
#[pyo3(signature = (path, generation=None, transport=None, endpoint=None))]
fn info_async<'py>(
    py: Python<'py>,
    path: String,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
        let entry = fs.info(&target).await.map_err(to_py_err)?;
        Python::attach(|py| Ok(entry_to_py_dict(py, &entry)?.into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (path, generation=None, transport=None, endpoint=None))]
fn info(
    py: Python<'_>,
    path: String,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<Py<PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    let entry = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
            fs.info(&target).await.map_err(to_py_err)
        })
    })?;
    Ok(entry_to_py_dict(py, &entry)?.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None, transport=None, endpoint=None))]
fn cat_file_async<'py>(
    py: Python<'py>,
    path: String,
    start: Option<i64>,
    end: Option<i64>,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
        let data = fs
            .cat_file(&target, ByteRange::new(start, end), ReadOptions::default())
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| Ok(PyBytes::new(py, &data).into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None, transport=None, endpoint=None))]
fn cat_file(
    py: Python<'_>,
    path: String,
    start: Option<i64>,
    end: Option<i64>,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<Py<PyBytes>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    let data = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
            fs.cat_file(&target, ByteRange::new(start, end), ReadOptions::default())
                .await
                .map_err(to_py_err)
        })
    })?;
    Ok(PyBytes::new(py, &data).into())
}

fn build_write_options(
    mode: Option<&str>,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    block_size: Option<usize>,
) -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.mode = match mode {
        Some("create") | Some("xb") | Some("x") => WriteMode::Create,
        _ => WriteMode::Overwrite,
    };
    opts.content_type = content_type;
    if let Some(meta) = metadata {
        opts.metadata = meta;
    }
    opts.block_size = block_size;
    opts
}

#[pyfunction]
#[pyo3(signature = (path, data, content_type=None, metadata=None, mode=None, transport=None, endpoint=None))]
fn pipe_file_async<'py>(
    py: Python<'py>,
    path: String,
    data: PyBackedBytes,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    mode: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let opts = build_write_options(mode.as_deref(), content_type, metadata, None);
    let payload = Bytes::from_owner(data);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
        fs.pipe_file(&path, payload, opts).await.map_err(to_py_err)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (path, data, content_type=None, metadata=None, mode=None, transport=None, endpoint=None))]
fn pipe_file(
    py: Python<'_>,
    path: String,
    data: PyBackedBytes,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    mode: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
) -> PyResult<()> {
    let opts = build_write_options(mode.as_deref(), content_type, metadata, None);
    let payload = Bytes::from_owner(data);
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
            fs.pipe_file(&path, payload, opts).await.map_err(to_py_err)
        })
    })
}

/// Streaming file handle wrapping `Box<dyn gcs_rust_fs::File>` for `GCSFile` read/write flows.
#[pyclass]
struct RustGcsFile {
    inner: Arc<AsyncMutex<Box<dyn File>>>,
}

#[pymethods]
impl RustGcsFile {
    #[staticmethod]
    #[pyo3(signature = (path, mode="rb", content_type=None, metadata=None, block_size=None, generation=None, transport=None, endpoint=None))]
    fn open_sync(
        py: Python<'_>,
        path: String,
        mode: &str,
        content_type: Option<String>,
        metadata: Option<HashMap<String, String>>,
        block_size: Option<usize>,
        generation: Option<String>,
        transport: Option<String>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let target = format_path_with_generation(&path, generation.as_deref());
        let mode_str = mode.to_string();
        let file = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let fs = get_fs(transport.as_deref(), endpoint.as_deref()).await?;
                let open_opts = if mode_str.contains('r') {
                    OpenOptions::read()
                } else {
                    let write_opts = build_write_options(
                        Some(&mode_str),
                        content_type,
                        metadata,
                        block_size,
                    );
                    OpenOptions::from_write(write_opts)
                };
                fs.open(&target, open_opts).await.map_err(to_py_err)
            })
        })?;
        Ok(Self {
            inner: Arc::new(AsyncMutex::new(file)),
        })
    }

    #[pyo3(signature = (size=None))]
    fn read<'py>(&self, py: Python<'py>, size: Option<usize>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = Arc::clone(&self.inner);
        let data = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                guard.read(size).await.map_err(to_py_err)
            })
        })?;
        Ok(PyBytes::new(py, &data))
    }

    #[pyo3(signature = (start=None, end=None))]
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let inner = Arc::clone(&self.inner);
        let data = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let guard = inner.lock().await;
                guard
                    .read_range(ByteRange::new(start, end))
                    .await
                    .map_err(to_py_err)
            })
        })?;
        Ok(PyBytes::new(py, &data))
    }

    fn write(&self, py: Python<'_>, data: PyBackedBytes) -> PyResult<usize> {
        let inner = Arc::clone(&self.inner);
        let written = data.len();
        let payload = Bytes::from_owner(data);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                guard.write(payload).await.map_err(to_py_err)?;
                Ok(written)
            })
        })
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                guard.flush().await.map_err(to_py_err)
            })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                guard.close().await.map_err(to_py_err)
            })
        })
    }
}

#[pymodule]
fn gcs_rust_fs_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_runtime();
    m.add_function(wrap_pyfunction!(info_async, m)?)?;
    m.add_function(wrap_pyfunction!(info, m)?)?;
    m.add_function(wrap_pyfunction!(cat_file_async, m)?)?;
    m.add_function(wrap_pyfunction!(cat_file, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_file_async, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_file, m)?)?;
    m.add_class::<RustGcsFile>()?;
    Ok(())
}
