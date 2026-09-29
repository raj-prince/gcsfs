# Non-POSIX Rust Backend Integration (`gcs_file_spec`)

This document sketches how `gcsfs` integrates directly with `gcs_file_spec` via two operations:
- `cat_file`: fetch raw bytes / byte ranges directly into memory over **gRPC**.
- `stat`: fetch object metadata (`size`, `generation`, `updated`, etc.) over **gRPC**.

---

## 1. Before vs After Flow

### `cat_file` (Data Read)

```
BEFORE: Pure Python (aiohttp)
─────────────────────────────
gcsfs.cat_file()
       │
       ▼
_cat_file_sequential() / _cat_file_concurrent()
       │
       ▼
aiohttp.ClientSession (HTTP GET)
  • Runs inside Python asyncio loop (GIL held)
  • Reads chunks into Python bytearray
  • Allocates and copies final bytes
       │
       ▼
Python bytes returned


AFTER: Rust Backend (gcs_file_spec via gRPC)
────────────────────────────────────────────
gcsfs.cat_file()
       │  (if read_backend == "rust")
       ▼
gcs_file_spec.cat_file_async()
       │
       ▼
Tokio Worker Pool (gRPC via Rust SDK open_object)
  • Streams byte ranges directly over gRPC
  • Network I/O runs outside Python's GIL
       │
       ▼
PyBytes returned directly
```

### `info` / `stat` (Metadata)

```
BEFORE:
gcsfs.info() ──> self._call("GET", JSON API) ──> Python JSON decode ──> dict

AFTER:
gcsfs.info() ──> gcs_file_spec.stat_async() ──> Rust StorageControl (gRPC) ──> dict
```

---

## 2. Python Integration (`gcsfs/core.py`)

`GCSFileSystem` delegates directly to `gcs_file_spec` when initialized with `read_backend="rust"`:

```python
# gcsfs/core.py

async def _cat_file(self, path, start=None, end=None, **kwargs):
    if self.read_backend == "rust":
        import gcs_file_spec
        generation = kwargs.get("generation")
        return await gcs_file_spec.cat_file_async(path, start=start, end=end, generation=generation)

    # ... existing aiohttp fallback ...

async def _info(self, path, generation=None, **kwargs):
    if self.read_backend == "rust":
        import gcs_file_spec
        try:
            return await gcs_file_spec.stat_async(path, generation=generation)
        except FileNotFoundError:
            pass

    # ... existing aiohttp fallback ...
```

---

## 3. Rust Backend Layout (`rust/gcs_file_spec/src/`)

The Rust crate is split into small, focused files:
- **`lib.rs`** (16 lines): module declaration and PyO3 registration.
- **`cat.rs`**: `cat_file_async` streaming over gRPC (`open_object`).
- **`stat.rs`**: `stat_async` metadata query over gRPC (`StorageControl`).
- **`client.rs`**: client singletons and Tokio runtime setup.
- **`common.rs`**: path parsing helper.

### Entry Point (`rust/gcs_file_spec/src/lib.rs`)

```rust
//! PyO3 entry point for the Non-POSIX `gcs_file_spec` module.

mod cat;
mod client;
mod common;
mod stat;

use pyo3::prelude::*;

#[pymodule]
fn gcs_file_spec(m: &Bound<'_, PyModule>) -> PyResult<()> {
    client::init_runtime();
    m.add_function(wrap_pyfunction!(cat::cat_file_async, m)?)?;
    m.add_function(wrap_pyfunction!(stat::stat_async, m)?)?;
    Ok(())
}
```

### `cat_file_async` via gRPC (`rust/gcs_file_spec/src/cat.rs`)
Opens the object descriptor over gRPC and streams range chunks directly into a `PyBytes` buffer:

```rust
async fn read_range(bucket: &str, object: &str, start: Option<u64>, end: Option<u64>, generation: Option<i64>) -> PyResult<Vec<u8>> {
    let client = grpc_client().await?;
    let mut builder = client.open_object(format!("projects/_/buckets/{bucket}"), object);
    if let Some(gen) = generation {
        builder = builder.set_generation(gen);
    }
    let desc = builder.send().await.map_err(|e| PyIOError::new_err(e.to_string()))?;

    let range = match (start, end) {
        (Some(s), Some(e)) if e > s => ReadRange::segment(s, e - s),
        (Some(s), None) => ReadRange::offset(s),
        (None, Some(e)) => ReadRange::segment(0, e),
        _ => ReadRange::all(),
    };

    let mut reader = desc.read_range(range).await;
    let mut contents = Vec::new();
    while let Some(res) = reader.next().await {
        contents.extend_from_slice(&res.map_err(|e| PyIOError::new_err(e.to_string()))?);
    }
    Ok(contents)
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None))]
pub fn cat_file_async<'py>(py: Python<'py>, path: String, start: Option<u64>, end: Option<u64>, generation: Option<i64>) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_gcs_path(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let contents = read_range(&bucket, &object, start, end, generation).await?;
        Python::attach(|py| Ok(PyBytes::new(py, &contents).into_any().unbind()))
    })
}
```

### `stat_async` via gRPC (`rust/gcs_file_spec/src/stat.rs`)
Fetches object properties via `StorageControl` (gRPC) and returns a dictionary compatible with `fsspec` metadata:

```rust
#[pyfunction]
#[pyo3(signature = (path, generation=None))]
pub fn stat_async<'py>(py: Python<'py>, path: String, generation: Option<i64>) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_gcs_path(&path)?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = control_client().await?;
        let obj = client.get_object()
            .set_bucket(format!("projects/_/buckets/{bucket}"))
            .set_object(&object)
            .send().await?;

        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("name", format!("{bucket}/{object}"))?;
            dict.set_item("size", obj.size)?;
            dict.set_item("generation", obj.generation)?;
            dict.set_item("type", "file")?;
            dict.set_item("updated", String::from(obj.update_time.unwrap()))?;
            Ok(dict.into_any().unbind())
        })
    })
}
```

---

## 4. How to Build & Run

```bash
# 1. Build & install extension into current venv
cd rust/gcs_file_spec
maturin develop --uv

# 2. Run in Python (uses gRPC backend)
fs = gcsfs.GCSFileSystem(read_backend="rust")

# Fetches metadata via Rust SDK over gRPC
info = fs.info("my-bucket/checkpoint.pt")

# Reads byte range over gRPC directly into bytes
data = fs.cat_file("my-bucket/checkpoint.pt", start=0, end=1024 * 1024)
```
