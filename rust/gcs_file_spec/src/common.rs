use pyo3::exceptions::PyValueError;
use pyo3::PyResult;

pub fn parse_gcs_path(path: &str) -> PyResult<(String, String)> {
    let clean = path.strip_prefix("gs://").unwrap_or(path).trim_start_matches('/');
    match clean.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(PyValueError::new_err(format!(
            "Invalid GCS path '{path}': expected 'bucket/object' or 'gs://bucket/object'"
        ))),
    }
}

