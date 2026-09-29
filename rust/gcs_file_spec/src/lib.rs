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
