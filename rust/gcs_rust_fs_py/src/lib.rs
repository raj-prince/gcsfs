//! Thin PyO3 bridge exposing `gcs-rust-fs` v0.1.1 (`CachedFs<GcsFs>` Mode 1 readahead and
//! `RemoteFs` UDS gRPC client) to `gcsfs`.

use bytes::Bytes;
use gcs_rust_fs::{
    ByteRange, CacheConfig, CachedFs, DataConfig, Entry, EntryKind, Error as FsError, ErrorKind,
    File, FileSystem, GcsFs, OpenOptions, ReadOptions, ReadaheadConfig, RemoteFs, Transport,
    WriteMode, WriteOptions,
};
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIOError, PyIsADirectoryError, PyNotADirectoryError,
    PyNotImplementedError, PyPermissionError, PyTimeoutError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyDict, PyNone};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
enum RustBackend {
    Direct(Arc<CachedFs<GcsFs>>),
    RemoteUds(Arc<RemoteFs>),
}

impl RustBackend {
    async fn info(&self, path: &str, uncached: bool) -> Result<Entry, FsError> {
        match self {
            Self::Direct(cached) => {
                if uncached {
                    cached.inner().info(path).await
                } else {
                    cached.info(path).await
                }
            }
            Self::RemoteUds(remote) => remote.info(path).await,
        }
    }

    async fn cat_file(
        &self,
        path: &str,
        range: ByteRange,
        opts: ReadOptions,
    ) -> Result<Bytes, FsError> {
        match self {
            Self::Direct(cached) => cached.cat_file(path, range, opts).await,
            Self::RemoteUds(remote) => remote.cat_file(path, range, opts).await,
        }
    }

    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<(), FsError> {
        match self {
            Self::Direct(cached) => cached.pipe_file(path, data, opts).await,
            Self::RemoteUds(remote) => remote.pipe_file(path, data, opts).await,
        }
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>, FsError> {
        match self {
            Self::Direct(cached) => cached.open(path, opts).await,
            Self::RemoteUds(remote) => remote.open(path, opts).await,
        }
    }
}

type CacheKey = (String, String, String);

static FS_CACHE: StdMutex<Option<HashMap<CacheKey, RustBackend>>> = StdMutex::new(None);

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn worker_threads() -> usize {
    env_usize("GCSFS_RUST_WORKER_THREADS", 32)
}

fn http_pool_size() -> usize {
    env_usize("GCS_RUST_FS_HTTP_POOL_SIZE", 48)
}

fn uds_pool_size() -> usize {
    env_usize("GCS_RUST_FS_UDS_POOL_SIZE", 16)
}

fn build_cache_config() -> CacheConfig {
    let io_size_mb = env_usize("GCS_RUST_FS_RA_IO_SIZE_MB", 16);
    let max_ra_mb = env_usize("GCS_RUST_FS_RA_MAX_READAHEAD_MB", 256);
    let max_inflight = env_usize("GCS_RUST_FS_RA_MAX_INFLIGHT", 24);
    let io_size = io_size_mb * 1024 * 1024;
    let max_readahead = max_ra_mb * 1024 * 1024;

    let mut cfg = CacheConfig::default();
    cfg.data = Some(DataConfig {
        readahead: ReadaheadConfig {
            page_size: 256 * 1024,
            io_size,
            max_folio_size: io_size,
            max_readahead,
            max_inflight,
            max_inflight_bytes: 4 * 1024 * 1024 * 1024,
        },
        cache: None,
    });
    cfg
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

async fn get_backend(
    transport: Option<&str>,
    endpoint: Option<&str>,
    uds_path: Option<&str>,
) -> PyResult<RustBackend> {
    let resolved_uds = uds_path
        .map(|s| s.trim().to_string())
        .or_else(|| std::env::var("GCSFS_RUST_UDS_PATH").ok())
        .filter(|s| !s.trim().is_empty());

    if let Some(sock_path) = resolved_uds {
        let key = ("uds".to_string(), String::new(), sock_path.clone());
        if let Some(existing) = FS_CACHE
            .lock()
            .expect("fs cache lock")
            .as_ref()
            .and_then(|m| m.get(&key).cloned())
        {
            return Ok(existing);
        }
        let remote = RemoteFs::connect_uds_with_pool_size(&sock_path, uds_pool_size())
            .await
            .map_err(to_py_err)?;
        let backend = RustBackend::RemoteUds(Arc::new(remote));
        let mut guard = FS_CACHE.lock().expect("fs cache lock");
        guard
            .get_or_insert_with(HashMap::new)
            .insert(key, backend.clone());
        return Ok(backend);
    }

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
        String::new(),
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
    builder = builder
        .transport(if trans_str == "http" {
            Transport::Http
        } else {
            Transport::Grpc
        })
        .http_pool_size(http_pool_size());
    if let Some(ep) = resolved_endpoint {
        builder = builder.endpoint(ep);
    }
    let gcs = builder.build().await.map_err(to_py_err)?;
    let cached = CachedFs::new(gcs, build_cache_config());
    let backend = RustBackend::Direct(Arc::new(cached));
    let mut guard = FS_CACHE.lock().expect("fs cache lock");
    guard
        .get_or_insert_with(HashMap::new)
        .insert(key, backend.clone());
    Ok(backend)
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
                if !stat.bucket.is_empty() {
                    dict.set_item("bucket", &stat.bucket)?;
                } else if let Some((bucket, _)) = entry.path.split_once('/') {
                    dict.set_item("bucket", bucket)?;
                }
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
#[pyo3(signature = (path, generation=None, transport=None, endpoint=None, uncached=false, uds_path=None))]
fn info_async<'py>(
    py: Python<'py>,
    path: String,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uncached: bool,
    uds_path: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_backend(
            transport.as_deref(),
            endpoint.as_deref(),
            uds_path.as_deref(),
        )
        .await?;
        let entry = fs.info(&target, uncached).await.map_err(to_py_err)?;
        Python::attach(|py| Ok(entry_to_py_dict(py, &entry)?.into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (path, generation=None, transport=None, endpoint=None, uncached=false, uds_path=None))]
fn info(
    py: Python<'_>,
    path: String,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uncached: bool,
    uds_path: Option<String>,
) -> PyResult<Py<PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    let entry = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_backend(
                transport.as_deref(),
                endpoint.as_deref(),
                uds_path.as_deref(),
            )
            .await?;
            fs.info(&target, uncached).await.map_err(to_py_err)
        })
    })?;
    Ok(entry_to_py_dict(py, &entry)?.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None, transport=None, endpoint=None, uds_path=None))]
fn cat_file_async<'py>(
    py: Python<'py>,
    path: String,
    start: Option<i64>,
    end: Option<i64>,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uds_path: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_backend(
            transport.as_deref(),
            endpoint.as_deref(),
            uds_path.as_deref(),
        )
        .await?;
        let data = fs
            .cat_file(&target, ByteRange::new(start, end), ReadOptions::default())
            .await
            .map_err(to_py_err)?;
        Python::attach(|py| Ok(PyBytes::new(py, &data).into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None, transport=None, endpoint=None, uds_path=None))]
fn cat_file(
    py: Python<'_>,
    path: String,
    start: Option<i64>,
    end: Option<i64>,
    generation: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uds_path: Option<String>,
) -> PyResult<Py<PyBytes>> {
    let target = format_path_with_generation(&path, generation.as_deref());
    let data = py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_backend(
                transport.as_deref(),
                endpoint.as_deref(),
                uds_path.as_deref(),
            )
            .await?;
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
#[pyo3(signature = (path, data, content_type=None, metadata=None, mode=None, transport=None, endpoint=None, uds_path=None))]
#[allow(clippy::too_many_arguments)]
fn pipe_file_async<'py>(
    py: Python<'py>,
    path: String,
    data: PyBackedBytes,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    mode: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uds_path: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let opts = build_write_options(mode.as_deref(), content_type, metadata, None);
    let payload = Bytes::from_owner(data);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fs = get_backend(
            transport.as_deref(),
            endpoint.as_deref(),
            uds_path.as_deref(),
        )
        .await?;
        fs.pipe_file(&path, payload, opts).await.map_err(to_py_err)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (path, data, content_type=None, metadata=None, mode=None, transport=None, endpoint=None, uds_path=None))]
#[allow(clippy::too_many_arguments)]
fn pipe_file(
    py: Python<'_>,
    path: String,
    data: PyBackedBytes,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    mode: Option<String>,
    transport: Option<String>,
    endpoint: Option<String>,
    uds_path: Option<String>,
) -> PyResult<()> {
    let opts = build_write_options(mode.as_deref(), content_type, metadata, None);
    let payload = Bytes::from_owner(data);
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let fs = get_backend(
                transport.as_deref(),
                endpoint.as_deref(),
                uds_path.as_deref(),
            )
            .await?;
            fs.pipe_file(&path, payload, opts).await.map_err(to_py_err)
        })
    })
}

/// Stateful file handle wrapping `Box<dyn gcs_rust_fs::File>` for `GCSFile` read/seek/write flows.
#[pyclass]
struct RustGcsFile {
    inner: Arc<AsyncMutex<Box<dyn File>>>,
    pos: AtomicU64,
    size: Option<u64>,
    entry: Option<Entry>,
}

#[pymethods]
impl RustGcsFile {
    #[staticmethod]
    #[pyo3(signature = (path, mode="rb", content_type=None, metadata=None, block_size=None, generation=None, transport=None, endpoint=None, uds_path=None))]
    #[allow(clippy::too_many_arguments)]
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
        uds_path: Option<String>,
    ) -> PyResult<Self> {
        let target = format_path_with_generation(&path, generation.as_deref());
        let mode_str = mode.to_string();
        let file = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let fs = get_backend(
                    transport.as_deref(),
                    endpoint.as_deref(),
                    uds_path.as_deref(),
                )
                .await?;
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
        let size = file.size();
        let pos = file.tell();
        let entry = size.map(|sz| Entry {
            path: file.path().to_string(),
            kind: EntryKind::File,
            size: sz,
            stat: file.stat().cloned(),
        });
        Ok(Self {
            inner: Arc::new(AsyncMutex::new(file)),
            pos: AtomicU64::new(pos),
            size,
            entry,
        })
    }

    #[pyo3(signature = (size=None))]
    fn read<'py>(&self, py: Python<'py>, size: Option<usize>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = Arc::clone(&self.inner);
        let (data, new_pos) = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                let bytes = guard.read(size).await.map_err(to_py_err)?;
                let pos = guard.tell();
                Ok::<(Bytes, u64), PyErr>((bytes, pos))
            })
        })?;
        self.pos.store(new_pos, Ordering::Relaxed);
        Ok(PyBytes::new(py, &data))
    }

    #[pyo3(signature = (pos, whence=0))]
    fn seek(&self, py: Python<'_>, pos: i64, whence: i32) -> PyResult<u64> {
        let seek_from = match whence {
            0 => {
                if pos < 0 {
                    return Err(PyValueError::new_err("negative seek position"));
                }
                SeekFrom::Start(pos as u64)
            }
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err(format!("invalid whence: {whence}"))),
        };
        let inner = Arc::clone(&self.inner);
        let new_pos = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut guard = inner.lock().await;
                guard.seek(seek_from).map_err(to_py_err)
            })
        })?;
        self.pos.store(new_pos, Ordering::Relaxed);
        Ok(new_pos)
    }

    fn tell(&self) -> PyResult<u64> {
        Ok(self.pos.load(Ordering::Relaxed))
    }

    fn size(&self) -> Option<u64> {
        self.size
    }

    fn info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.entry {
            Some(entry) => Ok(entry_to_py_dict(py, entry)?.into_any().unbind()),
            None => Ok(PyNone::get(py).to_owned().into_any().unbind()),
        }
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
                Ok::<usize, PyErr>(written)
            })
        })?;
        self.pos.fetch_add(written as u64, Ordering::Relaxed);
        Ok(written)
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
