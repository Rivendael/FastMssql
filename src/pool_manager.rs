use crate::pool_config::PyPoolConfig;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use parking_lot::Mutex;
use std::sync::Arc;
use tiberius::Config;

pub type ConnectionPool = Pool<ConnectionManager>;

pub async fn establish_pool(config: &Config, pool_config: &PyPoolConfig) -> PyResult<ConnectionPool> {
    let manager = ConnectionManager::new(config.clone());
    let mut builder = Pool::builder().retry_connection(true).max_size(pool_config.max_size);

    if let Some(min) = pool_config.min_idle { builder = builder.min_idle(Some(min)); }
    if let Some(lt) = pool_config.max_lifetime { builder = builder.max_lifetime(Some(lt)); }
    if let Some(to) = pool_config.idle_timeout { builder = builder.idle_timeout(Some(to)); }
    if let Some(ct) = pool_config.connection_timeout { builder = builder.connection_timeout(ct); }
    if let Some(test) = pool_config.test_on_check_out { builder = builder.test_on_check_out(test); }
    if let Some(retry) = pool_config.retry_connection { builder = builder.retry_connection(retry); }

    builder.build(manager).await.map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create connection pool: {}", e))
    })
}

pub async fn ensure_pool_initialized(
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: &PyPoolConfig,
) -> PyResult<ConnectionPool> {
    {
        let pool_guard = pool.lock();
        if let Some(ref p) = *pool_guard {
            return Ok(p.clone());
        }
    }
    
    let new_pool = establish_pool(&config, pool_config).await?;
    
    let mut pool_guard = pool.lock();
    match &*pool_guard {
        Some(ref p) => {
            Ok(p.clone())
        }
        None => {
            *pool_guard = Some(new_pool.clone());
            Ok(new_pool)
        }
    }
}