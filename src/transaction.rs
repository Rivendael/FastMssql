use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{timeout, Duration};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::azure_auth::PyAzureCredential;
use crate::batch::{execute_batch_on_connection, parse_batch_items, query_batch_on_connection};
use crate::helpers::wrap_query_stream;
use crate::parameter_conversion::{convert_parameters_to_fast, params_as_sql_refs};
use crate::ssl_config::PySslConfig;
use crate::types::{create_connection_error, create_sql_error, create_timeout_error};

/// Type for a single direct connection (not pooled)
type SingleConnectionType = Client<tokio_util::compat::Compat<TcpStream>>;

/// Bundles the three cloned handles needed for async transaction operations.
struct TransactionHandles {
    conn: Arc<AsyncMutex<Option<SingleConnectionType>>>,
    config: Arc<Config>,
    azure_credential: Option<PyAzureCredential>,
    query_timeout: Option<Duration>,
}

impl TransactionHandles {
    async fn ensure_connected(&self) -> PyResult<()> {
        Transaction::ensure_connected_inner(&self.conn, &self.config, self.azure_credential.as_ref()).await
    }
}

/// A single dedicated connection (not pooled) for transaction support.
/// This holds one physical database connection that persists across queries,
/// allowing SQL Server transactions (BEGIN/COMMIT/ROLLBACK) to work correctly.
#[pyclass(name = "Transaction")]
pub struct Transaction {
    conn: Arc<AsyncMutex<Option<SingleConnectionType>>>,
    config: Arc<Config>,
    _ssl_config: Option<PySslConfig>,
    azure_credential: Option<PyAzureCredential>,
    query_timeout: Option<Duration>,
}

#[pymethods]
impl Transaction {
    #[new]
    #[pyo3(signature = (connection_string = None, ssl_config = None, azure_credential = None, server = None, database = None, username = None, password = None, application_intent = None, port = None, instance_name = None, application_name = None, query_timeout = None))]
    pub fn new(
        connection_string: Option<String>,
        ssl_config: Option<PySslConfig>,
        azure_credential: Option<PyAzureCredential>,
        server: Option<String>,
        database: Option<String>,
        username: Option<String>,
        password: Option<String>,
        application_intent: Option<String>,
        port: Option<u16>,
        instance_name: Option<String>,
        application_name: Option<String>,
        query_timeout: Option<u64>,
    ) -> PyResult<Self> {
        // Store the original server parameter for validation before it gets reassigned
        let server_param = server.clone();

        let config = if let Some(conn_str) = connection_string {
            Config::from_ado_string(&conn_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?
        } else if let Some(srv) = server {
            let mut config = Config::new();
            config.host(&srv);
            if let Some(db) = database {
                config.database(&db);
            }
            if let Some(ref user) = username {
                if azure_credential.is_some() {
                    return Err(PyValueError::new_err(
                        "Cannot use both username/password and azure_credential. Choose one authentication method.",
                    ));
                }
                let pwd = password.ok_or_else(|| {
                    PyValueError::new_err("password is required when username is provided")
                })?;
                config.authentication(AuthMethod::sql_server(user, &pwd));
            } else if azure_credential.is_some() {
                // Azure authentication will be set up dynamically during connection
                // No authentication is set on config here since we need to acquire tokens asynchronously
            }
            if let Some(p) = port {
                config.port(p);
            }
            if let Some(itn) = instance_name {
                config.instance_name(itn);
            }
            if let Some(apn) = application_name {
                config.application_name(apn);
            }
            if let Some(intent) = application_intent {
                match intent.to_lowercase().trim() {
                    "readonly" | "read_only" => config.readonly(true),
                    "readwrite" | "read_write" | "" => config.readonly(false),
                    invalid => {
                        return Err(PyValueError::new_err(format!(
                            "Invalid application_intent '{}'. Valid values: 'readonly', 'read_only', 'readwrite', 'read_write', or empty string",
                            invalid
                        )));
                    }
                }
            }
            if let Some(ref ssl_cfg) = ssl_config {
                ssl_cfg.apply_to_config(&mut config);
            }
            config
        } else {
            return Err(PyValueError::new_err(
                "Either connection_string or server must be provided",
            ));
        };

        // Validate authentication configuration when using individual parameters
        if server_param.is_some() && username.is_none() && azure_credential.is_none() {
            return Err(PyValueError::new_err(
                "When using individual connection parameters, either username/password or azure_credential must be provided",
            ));
        }

        Ok(Transaction {
            conn: Arc::new(AsyncMutex::new(None)),
            config: Arc::new(config),
            _ssl_config: ssl_config,
            azure_credential,
            query_timeout: query_timeout.map(Duration::from_millis),
        })
    }

    /// Execute a SQL query that returns rows (SELECT statements)
    /// Returns rows as QueryStream
    #[pyo3(signature = (query, parameters=None))]
    pub fn query<'p>(
        &self,
        py: Python<'p>,
        query: String,
        parameters: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let fast_parameters = convert_parameters_to_fast(parameters, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                let execution_result = {
                    let tiberius_params = params_as_sql_refs(&fast_parameters);

                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before executing the query
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    let result = conn_ref
                        .query(&query, &tiberius_params)
                        .await
                        .map_err(|e| create_sql_error(e, "Query execution failed"))?
                        .into_first_result()
                        .await
                        .map_err(|e| create_sql_error(e, "Failed to get results"))?;

                    drop(conn_guard);
                    result
                };

                wrap_query_stream(execution_result)
            })
            .await
        })
    }

    /// Execute a raw (non-prepared statement) SQL query
    /// Returns rows as QueryStream
    #[pyo3(signature = (query))]
    pub fn simple_query<'p>(
        &self,
        py: Python<'p>,
        query: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                let execution_result = {
                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before executing the query
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    let result = conn_ref
                        .simple_query(&query)
                        .await
                        .map_err(|e| create_sql_error(e, "Query execution failed"))?
                        .into_first_result()
                        .await
                        .map_err(|e| create_sql_error(e, "Failed to get results"))?;

                    drop(conn_guard);
                    result
                };

                wrap_query_stream(execution_result)
            })
            .await
        })
    }

    /// Execute a SQL command that doesn't return rows (INSERT/UPDATE/DELETE/DDL)
    /// Returns the number of affected rows
    #[pyo3(signature = (command, parameters=None))]
    pub fn execute<'p>(
        &self,
        py: Python<'p>,
        command: String,
        parameters: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let fast_parameters = convert_parameters_to_fast(parameters, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                let affected = {
                    let tiberius_params = params_as_sql_refs(&fast_parameters);

                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before executing the command
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    let result = conn_ref
                        .execute(&command, &tiberius_params)
                        .await
                        .map_err(|e| create_sql_error(e, "Command execution failed"))?;

                    drop(conn_guard);
                    result.total()
                };

                Ok(affected)
            })
            .await
        })
    }

    /// Execute multiple batch commands on the transaction connection.
    /// Does NOT wrap in automatic transaction - use begin/commit/rollback manually.
    /// Returns list of row counts affected by each command.
    #[pyo3(signature = (commands))]
    pub fn execute_batch<'p>(
        &self,
        py: Python<'p>,
        commands: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let batch_commands = parse_batch_items(commands, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                let all_results = {
                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before executing batch commands
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    execute_batch_on_connection(conn_ref, batch_commands).await?
                };

                Python::attach(|py| {
                    let py_list = PyList::new(py, all_results)?;
                    Ok(py_list.into_any().unbind())
                })
            })
            .await
        })
    }

    /// Execute multiple batch queries on the transaction connection.
    /// Returns list of QueryStream objects, one per query.
    #[pyo3(signature = (queries))]
    pub fn query_batch<'p>(
        &self,
        py: Python<'p>,
        queries: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let batch_queries = parse_batch_items(queries, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                let all_results = {
                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before executing batch queries
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    query_batch_on_connection(conn_ref, batch_queries).await?
                };

                Python::attach(|py| -> PyResult<Py<PyAny>> {
                    let mut py_results = Vec::with_capacity(all_results.len());
                    for result in all_results {
                        let py_result = wrap_query_stream(result)?;
                        py_results.push(py_result.into_any());
                    }
                    let py_list = PyList::new(py, py_results)?;
                    Ok(py_list.into_any().unbind())
                })
            })
            .await
        })
    }

    /// Begin a transaction
    pub fn begin<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();

        future_into_py(py, async move {
            Self::apply_timeout(handles.query_timeout, async {
                handles.ensure_connected().await?;

                {
                    let mut conn_guard = handles.conn.lock().await;
                    let conn_ref = conn_guard
                        .as_mut()
                        .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                    // Validate that the connection is still alive before starting transaction
                    Self::validate_connection(conn_ref, &handles.conn).await?;

                    conn_ref
                        .simple_query("BEGIN TRANSACTION")
                        .await
                        .map_err(|e| create_sql_error(e, "Failed to begin transaction"))?;

                    drop(conn_guard);
                }

                Ok(())
            })
            .await
        })
    }

    /// Commit the current transaction
    pub fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);
        let query_timeout = self.query_timeout;

        future_into_py(py, async move {
            Self::apply_timeout(query_timeout, Self::execute_transaction_command(&conn, "COMMIT TRANSACTION", "Failed to commit transaction"))
            .await
        })
    }

    /// Rollback the current transaction
    pub fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);
        let query_timeout = self.query_timeout;

        future_into_py(py, async move {
            Self::apply_timeout(query_timeout, Self::execute_transaction_command(&conn, "ROLLBACK TRANSACTION", "Failed to rollback transaction"))
            .await
        })
    }

    /// Close the connection
    /// Attempts a best-effort rollback to clean up any open transactions on the server.
    /// Logs warnings if the rollback fails but still closes the connection.
    /// Always succeeds (returns Ok) since connection closure is the critical operation.
    pub fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);
        let query_timeout = self.query_timeout;

        future_into_py(py, async move {
            Self::apply_timeout(query_timeout, async {
                let mut conn_guard = conn.lock().await;
                if let Some(mut c) = conn_guard.take() {
                    // Attempt rollback to clean up any open transactions on the server.
                    // This prevents server-side resource leaks and held locks.
                    match c.simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await {
                        Ok(_) => {
                            // Rollback succeeded - transaction is cleaned up on server.
                        }
                        Err(e) => {
                            // Rollback failed: log the error so it's not silently ignored.
                            // This can happen if:
                            // - Connection is already broken (TCP stream disconnected)
                            // - Server closed the connection unexpectedly
                            // - The connection is in an invalid state
                            //
                            // We still close the connection below (best-effort cleanup).
                            // This log message will appear on stderr and can be captured by Python.
                            eprintln!(
                                "WARNING: Failed to rollback transaction during connection close: {}. \
                                The transaction may still be open on the server, potentially holding locks.",
                                e
                            );
                        }
                    }
                    // Connection is dropped here, closing the TCP stream.
                    // This ensures the connection is always closed regardless of rollback success.
                }
                Ok(())
            })
            .await
        })
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        // Only return true if we can acquire the lock and verify the connection exists.
        // If the lock is held by another task, we cannot reliably determine connection state,
        // so conservatively return false. This ensures the method accurately reflects whether
        // the connection is available and can be used.
        match self.conn.try_lock() {
            Ok(guard) => guard.is_some(),
            Err(_) => false,  // Lock held → cannot verify state, return conservative false
        }
    }
}

impl Transaction {
    /// Clone the three fields needed for async transaction operations into a single struct.
    fn clone_handles(&self) -> TransactionHandles {
        TransactionHandles {
            conn: Arc::clone(&self.conn),
            config: Arc::clone(&self.config),
            azure_credential: self.azure_credential.clone(),
            query_timeout: self.query_timeout,
        }
    }

    /// Validate that the connection is still alive by attempting a simple query.
    /// If the connection is stale or broken, closes it and returns an error.
    /// This prevents silent failures from using dead connections.
    ///
    /// # Arguments
    /// * `conn_ref` - Mutable reference to the connection
    /// * `conn` - Arc reference to the connection for cleanup on failure
    ///
    /// # Returns
    /// Ok(()) if connection is valid, Err with clear message if connection is dead
    async fn validate_connection(
        conn_ref: &mut SingleConnectionType,
        conn: &Arc<AsyncMutex<Option<SingleConnectionType>>>,
    ) -> PyResult<()> {
        // Attempt a simple query to verify the connection is still usable.
        // This catches stale TCP connections that exist as Some() but are no longer functional.
        let validation_result = conn_ref.simple_query("SELECT 1").await;

        match validation_result {
            Ok(_) => {
                // Connection is healthy, continue without dropping the lock
                Ok(())
            }
            Err(ref e) => {
                // Connection is stale or broken. Close it to allow reconnection on next use.
                let error_msg = e.to_string();
                
                let mut conn_guard = conn.lock().await;
                *conn_guard = None;
                drop(conn_guard);

                Err(PyRuntimeError::new_err(format!(
                    "Connection validation failed - connection is stale or broken: {}. \
                     Connection will be re-established on next operation.",
                    error_msg
                )))
            }
        }
    }

    /// Execute a transaction control command (BEGIN/COMMIT/ROLLBACK).
    async fn execute_transaction_command(
        conn: &Arc<AsyncMutex<Option<SingleConnectionType>>>,
        sql: &'static str,
        label: &'static str,
    ) -> PyResult<()> {
        let mut conn_guard = conn.lock().await;
        let conn_ref = conn_guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

        // Validate that the connection is still alive before executing transaction command
        Self::validate_connection(conn_ref, conn).await?;

        // Re-acquire the lock after validation (validate_connection may have closed it on error)
        drop(conn_guard);
        let mut conn_guard = conn.lock().await;
        let conn_ref = conn_guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

        conn_ref
            .simple_query(sql)
            .await
            .map_err(|e| create_sql_error(e, label))?;

        drop(conn_guard);
        Ok(())
    }

    /// Helper method to apply a timeout to an async operation
    /// Returns a timeout error if the operation takes longer than configured timeout
    async fn apply_timeout<F, T>(query_timeout: Option<Duration>, future: F) -> PyResult<T>
    where
        F: std::future::Future<Output = PyResult<T>>,
    {
        match query_timeout {
            Some(duration) => match timeout(duration, future).await {
                Ok(result) => result,
                Err(_) => {
                    Err(create_timeout_error(format!(
                        "Query execution exceeded timeout of {}ms",
                        duration.as_millis()
                    )))
                }
            },
            None => future.await,
        }
    }

    /// Ensure connection is established. Initializes connection if needed.
    /// Returns error if connection fails.
    async fn ensure_connected_inner(
        conn: &Arc<AsyncMutex<Option<SingleConnectionType>>>,
        config: &Arc<Config>,
        azure_credential: Option<&PyAzureCredential>,
    ) -> PyResult<()> {
        let mut conn_guard = conn.lock().await;
        if conn_guard.is_none() {
            let tcp_stream = TcpStream::connect(config.get_addr()).await.map_err(|e| {
                        create_connection_error(format!("Failed to connect to server: {}", e))
                    })?;

            // Disable Nagle algorithm — identical to pool connections in pool_manager.rs.
            // Without this, small TDS packets (common for parameterised queries) may be
            // buffered by the OS for up to 200 ms before transmission.
            tcp_stream.set_nodelay(true).map_err(|e| {
                create_connection_error(format!("Failed to set TCP_NODELAY: {}", e))
            })?;

            let compat_stream = tcp_stream.compat();

            // Configure authentication
            let mut auth_config = (**config).clone();
            if let Some(azure_cred) = azure_credential {
                let auth_method = azure_cred.to_auth_method().await?;
                auth_config.authentication(auth_method);
            }

            let new_conn: SingleConnectionType = Client::connect(auth_config, compat_stream)
                .await
                .map_err(|e| create_sql_error(e, "Failed to connect to database"))?;
            *conn_guard = Some(new_conn);
        }

        Ok(())
    }
}

