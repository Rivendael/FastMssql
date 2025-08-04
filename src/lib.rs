//! A high-performance Python library for Microsoft SQL Server using Rust and Tiberius

// Suppress the non_local_definitions warning for PyO3 macros
// This is a known issue with PyO3 macros and can be safely ignored
#![allow(non_local_definitions)]

use pyo3::prelude::*;

mod connection;
mod query;
mod types;
mod pool_config;
mod ssl_config;

pub use connection::PyConnection;
pub use query::PyQuery;
pub use types::{PyRow, PyValue, PyExecutionResult};
pub use pool_config::PyPoolConfig;
pub use ssl_config::{PySslConfig, EncryptionLevel};

/// A high-performance Python library for Microsoft SQL Server using Rust and Tiberius
#[pymodule]
fn fastmssql_core(_py: Python, m: &PyModule) -> PyResult<()> {
    // Initialize pyo3-asyncio for tokio with IO enabled
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);
    
    m.add_class::<PyConnection>()?;
    m.add_class::<PyQuery>()?;
    m.add_class::<PyRow>()?;
    m.add_class::<PyValue>()?;
    m.add_class::<PyExecutionResult>()?;
    m.add_class::<PyPoolConfig>()?;
    m.add_class::<PySslConfig>()?;
    m.add_class::<EncryptionLevel>()?;
    
    // Add module-level functions
    m.add_function(wrap_pyfunction!(version, m)?)?;
    
    Ok(())
}

/// Get the version of the mssql-python-rust library
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}