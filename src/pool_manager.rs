use crate::azure_auth::PyAzureCredential;
use crate::pool_config::PyPoolConfig;
use crate::types::create_connection_error;
use bb8::Pool;
use pyo3::prelude::*;
use std::fmt;
use std::sync::Arc;
use tiberius::Config;
use tokio::sync::RwLock;
use tokio_util::compat::TokioAsyncWriteCompatExt;

// ──────────────────────────────────────────────────────────────────────────────
// Custom connection manager
// ──────────────────────────────────────────────────────────────────────────────

type TiberiusClient = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

/// Error type for `AzureConnectionManager`.
#[derive(Debug)]
pub enum PoolConnectionError {
    Io(std::io::Error),
    Tiberius(tiberius::error::Error),
    Auth(String),
}

impl fmt::Display for PoolConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolConnectionError::Io(e) => write!(f, "I/O error: {e}"),
            PoolConnectionError::Tiberius(e) => write!(f, "SQL error: {e}"),
            PoolConnectionError::Auth(e) => write!(f, "Auth error: {e}"),
        }
    }
}

impl std::error::Error for PoolConnectionError {}

impl From<std::io::Error> for PoolConnectionError {
    fn from(e: std::io::Error) -> Self {
        PoolConnectionError::Io(e)
    }
}

impl From<tiberius::error::Error> for PoolConnectionError {
    fn from(e: tiberius::error::Error) -> Self {
        PoolConnectionError::Tiberius(e)
    }
}

/// A `bb8::ManageConnection` implementation that calls `to_auth_method()` on every
/// new physical connection.
///
/// For Azure credentials (`azure_credential = Some(…)`) this ensures the token cache
/// is consulted — and the token refreshed if it has expired — each time `bb8` opens a
/// connection (on pool warm-up, `max_lifetime` rotation, idle-timeout eviction, or
/// reconnect after error).  This fixes the bug where a static token baked into
/// `bb8_tiberius::ConnectionManager`'s config would silently go stale after ~1 hour.
///
/// For SQL Server / Windows auth (`azure_credential = None`) the base config already
/// carries the credentials and the manager behaves identically to `bb8_tiberius`.
pub struct AzureConnectionManager {
    /// Base config — host, port, database, SSL.  Auth is NOT set here for Azure paths;
    /// it is applied dynamically in `connect()`.
    base_config: Config,
    /// Azure credential, or `None` for non-Azure auth.
    azure_credential: Option<PyAzureCredential>,
}

impl AzureConnectionManager {
    pub fn new(base_config: Config, azure_credential: Option<PyAzureCredential>) -> Self {
        Self {
            base_config,
            azure_credential,
        }
    }
}

impl bb8::ManageConnection for AzureConnectionManager {
    type Connection = TiberiusClient;
    type Error = PoolConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut config = self.base_config.clone();

        // Refresh (or serve from cache) the Azure access token for every new connection.
        // `to_auth_method()` is cheap when a valid cached token exists; it only hits the
        // network when the token has expired.
        if let Some(cred) = &self.azure_credential {
            let auth_method = cred
                .to_auth_method()
                .await
                .map_err(|e| PoolConnectionError::Auth(e.to_string()))?;
            config.authentication(auth_method);
        }

        let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        let client = match tiberius::Client::connect(config.clone(), tcp.compat_write()).await {
            Ok(c) => c,
            // Server redirect: reconnect to the forwarded address.
            Err(tiberius::error::Error::Routing { host, port }) => {
                config.host(&host);
                config.port(port);
                let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
                tcp.set_nodelay(true)?;
                tiberius::Client::connect(config, tcp.compat_write()).await?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub type ConnectionPool = Pool<AzureConnectionManager>;

// ──────────────────────────────────────────────────────────────────────────────
// Pool helpers
// ──────────────────────────────────────────────────────────────────────────────

pub async fn establish_pool(
    base_config: &Config,
    azure_credential: Option<PyAzureCredential>,
    pool_config: &PyPoolConfig,
) -> PyResult<ConnectionPool> {
    let manager = AzureConnectionManager::new(base_config.clone(), azure_credential);
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
        .map_err(|e| create_connection_error(format!("Failed to create connection pool: {}", e)))?;

    // Warmup pool if min_idle is configured to eliminate cold-start latency.
    if let Some(min_idle) = pool_config.min_idle {
        warmup_pool(&pool, min_idle).await?;
    }

    Ok(pool)
}

pub async fn ensure_pool_initialized_with_auth(
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: &PyPoolConfig,
    azure_credential: Option<PyAzureCredential>,
) -> PyResult<ConnectionPool> {
    {
        let read_guard = pool.read().await;
        if let Some(existing_pool) = read_guard.as_ref() {
            return Ok(existing_pool.clone());
        }
    }

    let mut write_guard = pool.write().await;

    if let Some(existing_pool) = write_guard.as_ref() {
        return Ok(existing_pool.clone());
    }

    // Pass the base config and credential to establish_pool.
    // AzureConnectionManager will call to_auth_method() on every new connection,
    // so tokens are always fresh regardless of when bb8 decides to open them.
    let new_pool = establish_pool(&config, azure_credential, pool_config).await?;
    *write_guard = Some(new_pool.clone());
    drop(write_guard);

    Ok(new_pool)
}

/// Warms up the connection pool by pre-establishing `target_connections` connections.
/// This eliminates cold-start latency on first queries.
pub async fn warmup_pool(pool: &ConnectionPool, target_connections: u32) -> PyResult<()> {
    let mut handles = Vec::with_capacity(target_connections as usize);

    for _ in 0..target_connections {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            match pool_clone.get().await {
                Ok(_conn) => Ok(()),
                Err(e) => Err(e),
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .map_err(|e| create_connection_error(format!("Connection warmup task failed: {}", e)))?
            .map_err(|e| create_connection_error(format!("Connection warmup failed: {}", e)))?;
    }

    Ok(())
}
