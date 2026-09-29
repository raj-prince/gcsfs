use crate::client::grpc_client;
use crate::common::parse_gcs_path;
use google_cloud_storage::model_ext::ReadRange;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

async fn read_range(
    bucket: &str,
    object: &str,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Vec<u8>> {
    let client = grpc_client().await?;
    let b_path = format!("projects/_/buckets/{bucket}");
    let mut builder = client.open_object(&b_path, object);
    if let Some(gen) = generation {
        builder = builder.set_generation(gen);
    }
    let desc = builder
        .send()
        .await
        .map_err(|e| PyIOError::new_err(format!("GCS gRPC open failed for {object}: {e}")))?;

    let range = match (start, end) {
        (Some(s), Some(e)) if e > s => ReadRange::segment(s, e - s),
        (Some(s), Some(_)) => ReadRange::segment(s, 0),
        (Some(s), None) => ReadRange::offset(s),
        (None, Some(e)) => ReadRange::segment(0, e),
        (None, None) => ReadRange::all(),
    };

    let mut reader = desc.read_range(range).await;
    let mut contents = Vec::new();
    while let Some(res) = reader.next().await {
        let chunk = res.map_err(|e| PyIOError::new_err(format!("GCS gRPC read failed for {object}: {e}")))?;
        contents.extend_from_slice(&chunk);
    }
    Ok(contents)
}

#[pyfunction]
#[pyo3(signature = (path, start=None, end=None, generation=None))]
pub fn cat_file_async<'py>(
    py: Python<'py>,
    path: String,
    start: Option<u64>,
    end: Option<u64>,
    generation: Option<i64>,
) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_gcs_path(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let contents = read_range(&bucket, &object, start, end, generation).await?;
        Python::attach(|py| Ok(PyBytes::new(py, &contents).into_any().unbind()))
    })
}

