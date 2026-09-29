use crate::client::control_client;
use crate::common::parse_gcs_path;
use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyPermissionError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

async fn get_object_stat(
    bucket: &str,
    object: &str,
    generation: Option<i64>,
) -> PyResult<google_cloud_storage::model::Object> {
    let client = control_client().await?;
    let b_path = format!("projects/_/buckets/{bucket}");
    let mut builder = client.get_object().set_bucket(b_path).set_object(object);
    if let Some(gen) = generation {
        builder = builder.set_generation(gen);
    }
    builder.send().await.map_err(|e| {
        let err = e.to_string();
        if err.contains("NOT_FOUND") || err.contains("NotFound") || err.contains("404") {
            PyFileNotFoundError::new_err(format!("{bucket}/{object}"))
        } else if err.contains("PERMISSION_DENIED") || err.contains("403") {
            PyPermissionError::new_err(format!("{bucket}/{object}: {err}"))
        } else {
            PyIOError::new_err(format!("GCS get_object failed for {object}: {err}"))
        }
    })
}

fn object_to_py_dict<'py>(
    py: Python<'py>,
    obj: &google_cloud_storage::model::Object,
    bucket: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", format!("{}/{}", bucket, obj.name))?;
    dict.set_item("bucket", bucket)?;
    dict.set_item("size", obj.size)?;
    dict.set_item("type", "file")?;
    dict.set_item("generation", obj.generation)?;
    if let Some(ref update_time) = obj.update_time {
        dict.set_item("updated", String::from(*update_time))?;
    }
    Ok(dict)
}

#[pyfunction]
#[pyo3(signature = (path, generation=None))]
pub fn stat_async<'py>(
    py: Python<'py>,
    path: String,
    generation: Option<i64>,
) -> PyResult<Bound<'py, PyAny>> {
    let (bucket, object) = parse_gcs_path(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let obj = get_object_stat(&bucket, &object, generation).await?;
        Python::attach(|py| {
            let dict = object_to_py_dict(py, &obj, &bucket)?;
            Ok(dict.into_any().unbind())
        })
    })
}
