#![allow(non_local_definitions)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;

mod connection;
mod types;
mod py_parameters;
mod pool_config;
mod ssl_config;
mod pool_manager;
mod parameter_conversion;
mod batch;
mod type_mapping;

pub use connection::PyConnection;
pub use types::{PyFastRow, PyFastExecutionResult};
pub use py_parameters::{Parameter, Parameters};
pub use pool_config::PyPoolConfig;
pub use ssl_config::{PySslConfig, EncryptionLevel};

#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[pymodule]
fn fastmssql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);  // Fallback to 8 cores
    
    builder
        .enable_all()
        .worker_threads((cpu_count / 2).max(1).min(8))  // Fewer workers = less contention at high RPS
        .max_blocking_threads((cpu_count * 32).min(512)) // More blocking threads for DB I/O surge capacity
        .thread_keep_alive(std::time::Duration::from_secs(900)) // 15 minutes to avoid thrashing
        .thread_stack_size(4 * 1024 * 1024)  // Smaller stack = more threads, better for high concurrency
        .global_queue_interval(7)
        .event_interval(13);
    
    pyo3_async_runtimes::tokio::init(builder);
    
    m.add_class::<PyConnection>()?;
    m.add_class::<PyFastRow>()?;
    m.add_class::<PyFastExecutionResult>()?;
    m.add_class::<Parameter>()?;
    m.add_class::<Parameters>()?;
    m.add_class::<PyPoolConfig>()?;
    m.add_class::<PySslConfig>()?;
    m.add_class::<EncryptionLevel>()?;
    
    m.add_function(wrap_pyfunction!(version, m)?)?;
    
    Ok(())
}
