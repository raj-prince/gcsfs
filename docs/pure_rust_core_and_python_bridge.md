# Architecture: Pure Rust Core Crate & Thin Python Bridge

This document details the architecture for splitting the GCS Non-POSIX implementation into two distinct layers:
1. **`gcs_file_spec`**: An independent, pure Rust library crate (no Python dependencies).
2. **`gcsfs_bindings`**: A thin PyO3 bridge layer that exposes the Rust crate to `gcsfs`.

---

## 1. Architectural Overview

```
┌─────────────────────────────────────────────────────────────┐
│          gcs_file_spec (Independent Pure Rust Crate)        │
│                                                             │
│   • Has ZERO knowledge of Python, GIL, or PyO3              │
│   • Managed via standard Cargo (crates.io or Git repo)      │
│   • Native async APIs over gRPC:                            │
│       pub async fn cat_file(...) -> Result<Vec<u8>, Error>  │
│       pub async fn stat(...) -> Result<ObjectStat, Error>   │
└──────────────────────────────┬──────────────────────────────┘
                               │ Cargo dependency
                               ▼
┌─────────────────────────────────────────────────────────────┐
│             Thin Python Bridge (gcsfs/rust/)                │
│                                                             │
│   • Translates Rust types to Python runtime objects:        │
│       Vec<u8>    ──>  PyBytes                               │
│       ObjectStat ──>  PyDict                                │
│   • Initializes Tokio runtime and registers PyO3 module     │
└──────────────────────────────┬──────────────────────────────┘
                               │ Compiled C-Extension (.so)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                    gcsfs (Python)                           │
│                                                             │
│   • GCSFileSystem._cat_file() and _info() dispatch here     │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Component 1: Pure Rust Crate (`gcs_file_spec`)

This crate lives in its own repository and can be used by any Rust project.

### `Cargo.toml`
```toml
[package]
name = "gcs_file_spec"
version = "0.1.0"
edition = "2021"

[dependencies]
google-cloud-storage = "1"
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### Public API (`src/lib.rs`)
```rust
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;

pub struct ObjectStat {
    pub name: String,
    pub size: u64,
    pub generation: i64,
    pub updated: Option<String>,
}

/// Reads a byte range from GCS over gRPC into a native byte vector.
pub async fn cat_file(
    bucket: &str,
    object: &str,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let client = Storage::builder().build().await?;
    let mut builder = client.open_object(format!("projects/_/buckets/{bucket}"), object);
    if let Some(gen) = generation {
        builder = builder.set_generation(gen);
    }
    let desc = builder.send().await?;

    let range = match (start, end) {
        (Some(s), Some(e)) if e > s => ReadRange::segment(s, e - s),
        (Some(s), None) => ReadRange::offset(s),
        (None, Some(e)) => ReadRange::segment(0, e),
        _ => ReadRange::all(),
    };

    let mut reader = desc.read_range(range).await;
    let mut contents = Vec::new();
    while let Some(res) = reader.next().await {
        contents.extend_from_slice(&res?);
    }
    Ok(contents)
}

/// Fetches object metadata over gRPC.
pub async fn stat(
    bucket: &str,
    object: &str,
    generation: Option<i64>,
) -> Result<ObjectStat, Box<dyn std::error::Error + Send + Sync>> {
    let client = StorageControl::builder().build().await?;
    let mut builder = client
        .get_object()
        .set_bucket(format!("projects/_/buckets/{bucket}"))
        .set_object(object);
    if let Some(gen) = generation {
        builder = builder.set_generation(gen);
    }
    let obj = builder.send().await?;

    Ok(ObjectStat {
        name: format!("{bucket}/{object}"),
        size: obj.size,
        generation: obj.generation,
        updated: obj.update_time.map(String::from),
    })
}
```

---

## 3. Component 2: Thin Python Bridge (`gcsfs/rust/`)

Located within the `gcsfs` repository, this wrapper only handles the PyO3 boundary.

### `Cargo.toml`
```toml
[package]
name = "gcsfs_bindings"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
# Depend on the pure Rust crate
gcs_file_spec = { git = "https://github.com/your-org/gcs_file_spec", tag = "v0.1.0" }

pyo3 = { version = "0.29", features = ["extension-module"] }
pyo3-async-runtimes = { version = "0.29", features = ["tokio-runtime"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### Bridge Code (`src/lib.rs`)
```rust
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None))]
fn cat_file_async<'py>(
    py: Python<'py>,
    path: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_path(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let bytes = gcs_file_spec::cat_file(&bucket, &object, start, end, generation)
            .await
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Python::attach(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
    })
}

#[pyfunction]
#[pyo3(signature = (path, generation=None))]
fn stat_async<'py>(
    py: Python<'py>,
    path: String,
    generation: Option<i64>,
) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_path(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let meta = gcs_file_spec::stat(&bucket, &object, generation)
            .await
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("name", meta.name)?;
            dict.set_item("size", meta.size)?;
            dict.set_item("generation", meta.generation)?;
            dict.set_item("type", "file")?;
            dict.set_item("updated", meta.updated)?;
            Ok(dict.into_any().unbind())
        })
    })
}

#[pymodule]
fn gcsfs_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(16).enable_all();
    pyo3_async_runtimes::tokio::init(builder);

    m.add_function(wrap_pyfunction!(cat_file_async, m)?)?;
    m.add_function(wrap_pyfunction!(stat_async, m)?)?;
    Ok(())
}
```

---

## 4. Component 3: Calling from Python (`gcsfs/core.py`)

In `gcsfs`, the integration remains direct and clean:

```python
# gcsfs/core.py

async def _cat_file(self, path, start=None, end=None, **kwargs):
    if self.read_backend in ("grpc", "rust"):
        import gcsfs_bindings
        return await gcsfs_bindings.cat_file_async(
            path, start=start, end=end, generation=kwargs.get("generation")
        )

async def _info(self, path, generation=None, **kwargs):
    if self.read_backend in ("grpc", "rust"):
        import gcsfs_bindings
        return await gcsfs_bindings.stat_async(path, generation=generation)
```

---

## 5. Release & Maintenance Workflow

### Releasing `gcs_file_spec` (Pure Rust)
- Developed, benchmarked, and tested with standard `cargo test` and `cargo bench`.
- Tagged with standard semver (e.g., `git tag v0.1.0 && cargo publish`).
- **No Python wheel compilation matrices** needed in this repository.

### Releasing `gcsfs`
- When `gcsfs` updates to a new `gcs_file_spec` version, update the dependency in `gcsfs/rust/Cargo.toml`:
  ```toml
  gcs_file_spec = "0.2.0"
  ```
- Wheel building and binary distribution are isolated entirely to the bridge layer.

---

## 6. Key Advantages of this Boundary

1. **Ecosystem Reusability**: The core GCS gRPC logic can be directly used in any Rust application, CLI, or microservice without pulling in Python headers or the PyO3 runtime.
2. **Separation of Concerns**: Rust logic focuses strictly on GCS gRPC performance and streaming; the Python bridge focuses only on PyO3 conversion.
3. **Isolated Testing**: Core storage logic can be verified purely with Rust native test harnesses (`cargo test`) without requiring a Python interpreter or virtual environment.

