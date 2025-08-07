//! A high-performance Python library for Microsoft SQL Server using Rust and Tiberius

// Suppress the non_local_definitions warning for PyO3 macros
// This is a known issue with PyO3 macros and can be safely ignored
#![allow(non_local_definitions)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
    // OPTIMIZATION: Database-optimized Tokio runtime configuration
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    
    // Detect optimal core count for database I/O workloads
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);  // Fallback to 8 cores
    
    builder
        .enable_all()
        // CRITICAL: Ultra-tuned for 20K+ RPS database workloads
        .worker_threads((cpu_count / 4).max(1).min(4))  // Fewer workers = less contention at high RPS
        .max_blocking_threads((cpu_count * 32).min(512)) // More blocking threads for DB I/O surge capacity
        .thread_keep_alive(std::time::Duration::from_secs(900)) // 15 minutes to avoid thrashing
        .thread_stack_size(4 * 1024 * 1024)  // Smaller stack = more threads, better for high concurrency
        // CRITICAL: Ultra-aggressive scheduling for maximum RPS
        .global_queue_interval(7)   // Reduced from 31 - faster work stealing at high RPS
        .event_interval(13);        // Reduced from 61 - faster I/O polling
    
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
