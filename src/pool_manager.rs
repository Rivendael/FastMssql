use crate::pool_config::PyPoolConfig;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use tiberius::Config;
use tokio::sync::OnceCell;

pub type ConnectionPool = Pool<ConnectionManager>;

pub async fn establish_pool(
    config: &Config,
    pool_config: &PyPoolConfig,
) -> PyResult<ConnectionPool> {
    let manager = ConnectionManager::new(config.clone());
    let mut builder = Pool::builder()
        .retry_connection(true)
        .max_size(pool_config.max_size);

    if let Some(min) = pool_config.min_idle {
        builder = builder.min_idle(Some(min));
    }
    if let Some(lt) = pool_config.max_lifetime {
        builder = builder.max_lifetime(Some(lt));
    }
    if let Some(to) = pool_config.idle_timeout {
        builder = builder.idle_timeout(Some(to));
    }
    if let Some(ct) = pool_config.connection_timeout {
        builder = builder.connection_timeout(ct);
    }
    if let Some(test) = pool_config.test_on_check_out {
        builder = builder.test_on_check_out(test);
    }
    if let Some(retry) = pool_config.retry_connection {
        builder = builder.retry_connection(retry);
    }

    let pool = builder
        .build(manager)
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create connection pool: {}", e)))?;

    // Warmup pool if min_idle is configured to eliminate cold-start latency
    if let Some(min_idle) = pool_config.min_idle {
        warmup_pool(&pool, min_idle).await?;
    }

    Ok(pool)
}

pub async fn ensure_pool_initialized(
    pool: Arc<OnceCell<ConnectionPool>>,
    config: Arc<Config>,
    pool_config: &PyPoolConfig,
) -> PyResult<Arc<ConnectionPool>> {
    pool.get_or_try_init(|| async { establish_pool(&config, pool_config).await })
        .await
        .map(|p| Arc::new(p.clone()))
}

/// Warms up the connection pool by pre-establishing min_idle connections
/// This eliminates cold-start latency on first queries
pub async fn warmup_pool(pool: &ConnectionPool, target_connections: u32) -> PyResult<()> {
    // Pre-spawn connections sequentially (fast enough for initialization)
    for _ in 0..target_connections {
        let _ = pool
            .get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Connection warmup failed: {}", e)))?;
        // Connection is automatically returned to pool when dropped
    }

    Ok(())
}
