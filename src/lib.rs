//! A high-performance Python library for Microsoft SQL Server using Rust and Tiberius

// Suppress the non_local_definitions warning for PyO3 macros
// This is a known issue with PyO3 macros and can be safely ignored
#![allow(non_local_definitions)]

use pyo3::prelude::*;

mod connection;
mod optimized_types;
mod parameters;
mod pool_config;
mod ssl_config;

pub use connection::PyConnection;
pub use optimized_types::{PyFastRow, PyFastExecutionResult};
pub use parameters::{Parameter, Parameters};
pub use pool_config::PyPoolConfig;
pub use ssl_config::{PySslConfig, EncryptionLevel};

/// Get the library version
#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// The PyO3 module registration
#[pymodule]
fn fastmssql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize pyo3-async-runtimes for tokio with high-performance settings
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    
    // Detect CPU count or use sensible defaults
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);  // Fallback to 8 cores
    
    builder
        .enable_all()
        .worker_threads(cpu_count.max(12))  // Even more worker threads
        .max_blocking_threads(256)  // MUCH higher for database I/O - this is key!
        .thread_keep_alive(std::time::Duration::from_secs(120))  // Keep threads alive even longer
        .thread_stack_size(4 * 1024 * 1024);  // 4MB stack for each thread
    
    pyo3_async_runtimes::tokio::init(builder);
    
    m.add_class::<PyConnection>()?;
    m.add_class::<PyFastRow>()?;
    m.add_class::<PyFastExecutionResult>()?;
    m.add_class::<Parameter>()?;
    m.add_class::<Parameters>()?;
    m.add_class::<PyPoolConfig>()?;
    m.add_class::<PySslConfig>()?;
    m.add_class::<EncryptionLevel>()?;
    
    // Add module-level functions
    m.add_function(wrap_pyfunction!(version, m)?)?;
    
    Ok(())
}
