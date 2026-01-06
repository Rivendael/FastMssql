#![allow(non_local_definitions)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;

mod batch;
mod connection;
mod parameter_conversion;
mod pool_config;
mod pool_manager;
mod py_parameters;
mod ssl_config;
mod transaction;
mod type_mapping;
mod types;

pub use connection::PyConnection;
pub use pool_config::PyPoolConfig;
pub use py_parameters::{Parameter, Parameters};
pub use ssl_config::{EncryptionLevel, PySslConfig};
pub use transaction::Transaction;
pub use types::{PyFastRow, PyQueryStream};

#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[pymodule]
fn fastmssql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8); // Fallback to 8 cores

    builder
        .enable_all()
        .worker_threads((cpu_count * 2).max(4).min(512)) // 2x CPUs for I/O-bound database work
        .max_blocking_threads((cpu_count * 64).min(1024)) // More blocking threads for DB I/O surge capacity
        .thread_keep_alive(std::time::Duration::from_secs(900)) // 15 minutes to avoid thrashing
        .thread_stack_size(2 * 1024 * 1024) // 2MB stack = 2x more threads, optimal for high concurrency
        .global_queue_interval(31) // Lower frequency = better locality, less stealing overhead
        .event_interval(61); // Less frequent epoll = more batching

    pyo3_async_runtimes::tokio::init(builder);

    m.add_class::<PyConnection>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<PyFastRow>()?;
    m.add_class::<PyQueryStream>()?;
    m.add_class::<Parameter>()?;
    m.add_class::<Parameters>()?;
    m.add_class::<PyPoolConfig>()?;
    m.add_class::<PySslConfig>()?;
    m.add_class::<EncryptionLevel>()?;

    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}
