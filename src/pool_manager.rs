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

    builder.build(manager).await.map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create connection pool: {}", e))
    })
}

/// Ensures the connection pool is initialized, either by reusing an existing one
/// or creating a new one if needed.
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
    // Double-check: another thread might have initialized while we were setting up
    if let Some(ref p) = *pool_guard {
        // Return the already-initialized pool that another thread created
        Ok(p.clone())
    } else {
        // Store and return the newly created pool (move, not clone)
        *pool_guard = Some(new_pool.clone());
        Ok(new_pool)
    }
}