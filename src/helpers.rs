use pyo3::prelude::*;
use tiberius::Row;

/// Wrap `Vec<Row>` into a `Py<PyAny>` via `PyQueryStream`.
/// Shared between connection.rs and transaction.rs.
pub fn wrap_query_stream(rows: Vec<Row>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| -> PyResult<Py<PyAny>> {
        let query_stream = crate::types::PyQueryStream::from_tiberius_rows(rows, py)?;
        let py_result = Py::new(py, query_stream)?;
        Ok(py_result.into_any())
    })
}
