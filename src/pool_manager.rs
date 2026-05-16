use crate::azure_auth::PyAzureCredential;
use crate::pool_config::PyPoolConfig;
use crate::types::{create_connection_error, create_sql_error};
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

/// Convert a [`PoolConnectionError`] into a typed Python exception,
/// preserving the structured context of the underlying [`tiberius::error::Error`]
/// (SQL error code/state, TLS details, routing info, etc.) rather than
/// collapsing everything into an opaque string via `Display`.
impl From<PoolConnectionError> for pyo3::PyErr {
    fn from(e: PoolConnectionError) -> Self {
        match e {
            PoolConnectionError::Tiberius(terr) => create_sql_error(terr, "Connection error"),
            PoolConnectionError::Io(err) => create_connection_error(format!("I/O error: {err}")),
            PoolConnectionError::Auth(msg) => {
                create_connection_error(format!("Authentication error: {msg}"))
            }
        }
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
        // Roll back any uncommitted transaction that might have leaked onto this
        // connection (e.g., future dropped between BEGIN and COMMIT), then confirm
        // the connection is still alive — combined into a single round-trip.
        // This runs only when test_on_check_out = true or on periodic lifetime /
        // idle-timeout health checks — never on every routine checkout.
        conn.simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; SELECT 1")
            .await?;
        Ok(())
    }

    /// Returns `false` unconditionally.
    ///
    /// bb8 calls this synchronously on every connection return.  `tiberius::Client`
    /// wraps an async TCP stream and exposes no synchronous liveness check, so
    /// there is nothing meaningful to inspect here.  All real health-checking is
    /// handled by [`is_valid`](AzureConnectionManager::is_valid), which runs a
    /// real server round-trip on periodic lifetime / idle-timeout checks and,
    /// optionally, on every checkout when `test_on_check_out = true`.
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
    let mut builder = Pool::builder().max_size(pool_config.max_size);

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

    let pool = builder.build(manager).await.map_err(pyo3::PyErr::from)?;

    // Warmup pool if min_idle is configured to eliminate cold-start latency.
    if let Some(min_idle) = pool_config.min_idle {
        // Derive per-connection budget from pool_config; fall back to 30 s.
        let conn_timeout = pool_config
            .connection_timeout
            .unwrap_or(std::time::Duration::from_secs(30));
        warmup_pool(&pool, min_idle, conn_timeout).await?;
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
    Ok(new_pool)
}

/// Warms up the connection pool by pre-establishing `target_connections` connections.
/// This eliminates cold-start latency on first queries.
///
/// All tasks run concurrently via a [`tokio::task::JoinSet`].  The total budget is
/// `connection_timeout × target_connections` (capped at 120 s).  If the deadline
/// expires, all outstanding tasks are cancelled via [`JoinSet::shutdown`] and an
/// error is returned.  All individual errors are collected and surfaced together
/// rather than bailing on the first failure.
pub async fn warmup_pool(
    pool: &ConnectionPool,
    target_connections: u32,
    connection_timeout: std::time::Duration,
) -> PyResult<()> {
    use tokio::task::JoinSet;

    // Total warmup budget: per-connection timeout × number of connections, capped at
    // 2 minutes.  bb8 will enforce connection_timeout per task when calling
    // pool.get(); this outer deadline is a safety net to guarantee that
    // warmup_pool() always returns even if bb8's own timeout is misconfigured or
    // bypassed.
    let warmup_budget =
        (connection_timeout * target_connections.max(1)).min(std::time::Duration::from_secs(120));

    let mut set: JoinSet<Result<(), bb8::RunError<PoolConnectionError>>> = JoinSet::new();

    for _ in 0..target_connections {
        let pool_clone = pool.clone();
        // Each task acquires one connection (exercising the full connect path) then
        // immediately drops the guard, returning it to the pool.
        set.spawn(async move { pool_clone.get().await.map(|_conn| ()) });
    }

    let deadline = tokio::time::Instant::now() + warmup_budget;
    let mut errors: Vec<String> = Vec::new();

    loop {
        match tokio::time::timeout_at(deadline, set.join_next()).await {
            // Task completed successfully.
            Ok(Some(Ok(Ok(())))) => {}
            // Task returned a bb8/connection error – collect it and continue.
            Ok(Some(Ok(Err(e)))) => errors.push(e.to_string()),
            // Task panicked or was cancelled – record the join error and continue.
            Ok(Some(Err(join_err))) => errors.push(format!("task panicked: {join_err}")),
            // All tasks finished.
            Ok(None) => break,
            // Overall deadline exceeded – abort every outstanding task.
            Err(_elapsed) => {
                let outstanding = set.len();
                set.shutdown().await;
                return Err(create_connection_error(format!(
                    "Connection pool warmup timed out after {}s ({outstanding} task(s) cancelled)",
                    warmup_budget.as_secs(),
                )));
            }
        }
    }

    if !errors.is_empty() {
        return Err(create_connection_error(format!(
            "Connection pool warmup encountered {} error(s): {}",
            errors.len(),
            errors.join("; "),
        )));
    }

    Ok(())
}
