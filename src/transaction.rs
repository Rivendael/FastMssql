use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::azure_auth::PyAzureCredential;
use crate::batch::{execute_batch_on_connection, parse_batch_items, query_batch_on_connection};
use crate::parameter_conversion::convert_parameters_to_fast;
use crate::ssl_config::PySslConfig;
use crate::types::{create_connection_error, create_sql_error};

/// Type for a single direct connection (not pooled)
type SingleConnectionType = Client<tokio_util::compat::Compat<TcpStream>>;

/// A single dedicated connection (not pooled) for transaction support.
/// This holds one physical database connection that persists across queries,
/// allowing SQL Server transactions (BEGIN/COMMIT/ROLLBACK) to work correctly.
#[pyclass(name = "Transaction")]
pub struct Transaction {
    conn: Arc<AsyncMutex<Option<SingleConnectionType>>>,
    config: Arc<Config>,
    _ssl_config: Option<PySslConfig>,
    azure_credential: Option<PyAzureCredential>,
}

#[pymethods]
impl Transaction {
    #[new]
    #[pyo3(signature = (connection_string = None, ssl_config = None, azure_credential = None, server = None, database = None, username = None, password = None, application_intent = None, port = None, instance_name = None, application_name = None))]
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
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            // Execute query on the held connection
            let execution_result = {
                let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = fast_parameters
                    .iter()
                    .map(|p| p as &dyn tiberius::ToSql)
                    .collect();

                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                let result = conn_ref
                    .query(&query, &tiberius_params)
                    .await
                    .map_err(|e| create_sql_error(e, "Query execution failed"))?
                    .into_first_result()
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to get results: {}", e))
                    })?;

                drop(conn_guard); // Release lock after consuming all results
                result
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let query_stream =
                    crate::types::PyQueryStream::from_tiberius_rows(execution_result, py)?;
                let py_result = Py::new(py, query_stream)?;
                Ok(py_result.into_any())
            })
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
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            // Execute query on the held connection
            let execution_result = {
                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                let result = conn_ref
                    .simple_query(&query)
                    .await
                    .map_err(|e| create_sql_error(e, "Query execution failed"))?
                    .into_first_result()
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to get results: {}", e))
                    })?;

                drop(conn_guard); // Release lock after consuming all results
                result
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let query_stream =
                    crate::types::PyQueryStream::from_tiberius_rows(execution_result, py)?;
                let py_result = Py::new(py, query_stream)?;
                Ok(py_result.into_any())
            })
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
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            // Execute command on the held connection
            let affected = {
                let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = fast_parameters
                    .iter()
                    .map(|p| p as &dyn tiberius::ToSql)
                    .collect();

                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                let result = conn_ref
                    .execute(&command, &tiberius_params)
                    .await
                    .map_err(|e| create_sql_error(e, "Command execution failed"))?;

                drop(conn_guard); // Release lock

                result.total()
            };

            Ok(affected)
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
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            let all_results = {
                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                execute_batch_on_connection(conn_ref, batch_commands).await?
            };

            Python::attach(|py| {
                let py_list = PyList::new(py, all_results)?;
                Ok(py_list.into_any().unbind())
            })
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
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            let all_results = {
                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                query_batch_on_connection(conn_ref, batch_queries).await?
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let mut py_results = Vec::with_capacity(all_results.len());
                for result in all_results {
                    let query_stream = crate::types::PyQueryStream::from_tiberius_rows(result, py)?;
                    let py_result = Py::new(py, query_stream)?;
                    py_results.push(py_result.into_any());
                }
                let py_list = PyList::new(py, py_results)?;
                Ok(py_list.into_any().unbind())
            })
        })
    }

    /// Begin a transaction
    pub fn begin<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);
        let config = Arc::clone(&self.config);
        let azure_credential = self.azure_credential.clone();

        future_into_py(py, async move {
            Self::ensure_connected(&conn, &config, azure_credential.as_ref()).await?;

            // Execute BEGIN TRANSACTION
            {
                let mut conn_guard = conn.lock().await;
                let conn_ref = conn_guard
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

                conn_ref
                    .simple_query("BEGIN TRANSACTION")
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to begin transaction: {}", e))
                    })?;

                drop(conn_guard);
            }

            Ok(())
        })
    }

    /// Commit the current transaction
    pub fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);

        future_into_py(py, async move {
            let mut conn_guard = conn.lock().await;
            let conn_ref = conn_guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

            conn_ref
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to commit transaction: {}", e))
                })?;

            drop(conn_guard);
            Ok(())
        })
    }

    /// Rollback the current transaction
    pub fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);

        future_into_py(py, async move {
            let mut conn_guard = conn.lock().await;
            let conn_ref = conn_guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("Connection is not established"))?;

            conn_ref
                .simple_query("ROLLBACK TRANSACTION")
                .await
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to rollback transaction: {}", e))
                })?;

            drop(conn_guard);
            Ok(())
        })
    }

    /// Close the connection
    pub fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);

        future_into_py(py, async move {
            {
                let mut conn_guard = conn.lock().await;
                if let Some(mut c) = conn_guard.take() {
                    // Best-effort rollback: silently ignore errors (connection may already be
                    // broken or no transaction may be active — both are fine).
                    let _ = c.simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await;
                    // Connection is dropped here, closing the TCP stream.
                }
            }

            Ok(())
        })
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        // Derive connectivity from the actual connection object rather than a stale flag.
        // If the lock is held (query in progress), the connection is active → true.
        // If we can peek and it's Some, connected. If None, not connected.
        match self.conn.try_lock() {
            Ok(guard) => guard.is_some(),
            Err(_) => true,
        }
    }
}

impl Transaction {
    /// Ensure connection is established. Initializes connection if needed.
    /// Returns error if connection fails.
    async fn ensure_connected(
        conn: &Arc<AsyncMutex<Option<SingleConnectionType>>>,
        config: &Arc<Config>,
        azure_credential: Option<&PyAzureCredential>,
    ) -> PyResult<()> {
        // Establish connection if not already connected
        {
            let mut conn_guard = conn.lock().await;
            if conn_guard.is_none() {
                let tcp_stream = TcpStream::connect(config.get_addr()).await.map_err(|e| {
                    create_connection_error(format!("Failed to connect to server: {}", e))
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
        }

        Ok(())
    }
}

