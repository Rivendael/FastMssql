#![allow(non_local_definitions)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::prelude::*;

mod azure_auth;
mod batch;
mod connection;
mod helpers;
mod parameter_conversion;
mod pool_config;
mod pool_manager;
mod py_parameters;
mod ssl_config;
mod transaction;
mod type_mapping;
mod types;

pub use azure_auth::{AzureCredentialType, PyAzureCredential};
pub use connection::PyConnection;
pub use pool_config::PyPoolConfig;
pub use py_parameters::{Parameter, Parameters};
pub use ssl_config::{EncryptionLevel, PySslConfig};
pub use transaction::Transaction;
pub use types::{PyFastRow, PyQueryStream, SqlError, SqlConnectionError, TlsError, ProtocolError, ConversionError};

use crate::parameter_conversion::TypedNull;

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
        // Async I/O workload: 1× CPU workers is optimal. More workers increase work-stealing
        // contention without improving throughput for DB-latency-bound operations.
        .worker_threads(cpu_count.max(4).min(16))
        // No spawn_blocking is used anywhere in this codebase — all DB I/O is async.
        // A small ceiling gives a safety margin for any future sync work without
        // ballooning virtual memory (2 MB stack × N threads).
        .max_blocking_threads((cpu_count * 2).min(32))
        // 60 s amortises burst thread creation while releasing idle threads promptly.
        // The previous 900 s value kept surge threads alive for 15 minutes.
        .thread_keep_alive(std::time::Duration::from_secs(60))
        .thread_stack_size(2 * 1024 * 1024) // 2 MB — matches Tokio's recommendation
        // Tokio default (61). Smaller values cause excessive global-queue polling;
        // the previous value of 31 doubled poll frequency with no measured benefit.
        .global_queue_interval(61)
        .event_interval(61); // Tokio default — batches I/O event polling per scheduler tick

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
    m.add_class::<PyAzureCredential>()?;
    m.add_class::<AzureCredentialType>()?;
    m.add_class::<TypedNull>()?;
    
    {
        let py = m.py();
        m.add("SqlError", py.get_type::<SqlError>())?;
        m.add("SqlConnectionError", py.get_type::<SqlConnectionError>())?;
        m.add("TlsError", py.get_type::<TlsError>())?;
        m.add("ProtocolError", py.get_type::<ProtocolError>())?;
        m.add("ConversionError", py.get_type::<ConversionError>())?;
    }

    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}
