//! A high-performance Python library for Microsoft SQL Server using Rust and Tiberius

// Suppress the non_local_definitions warning for PyO3 macros
// This is a known issue with PyO3 macros and can be safely ignored
#![allow(non_local_definitions)]

use pyo3::prelude::*;

mod connection;
mod query;
mod types;

pub use connection::PyConnection;
pub use query::PyQuery;
pub use types::{PyRow, PyValue};

/// A high-performance Python library for Microsoft SQL Server using Rust and Tiberius
#[pymodule]
fn mssql_python_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    // Initialize pyo3-asyncio for tokio with IO enabled
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);
    
    m.add_class::<PyConnection>()?;
    m.add_class::<PyQuery>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyValue>()?;
    
    // Add module-level functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    
    Ok(())
}

/// Connect to a Microsoft SQL Server database
/// 
/// Args:
///     connection_string (str): The connection string for the database
///     
/// Returns:
///     PyConnection: A connection object for executing queries
///     
/// Example:
///     >>> import mssql_python_rust
///     >>> conn = mssql_python_rust.connect("Server=localhost;Database=test;Integrated Security=true")
#[pyfunction]
fn connect(connection_string: String) -> PyResult<PyConnection> {
    PyConnection::new(connection_string)
}

/// Get the version of the mssql-python-rust library
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}