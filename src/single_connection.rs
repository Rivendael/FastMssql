use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, Row, Client};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::parameter_conversion::{convert_parameters_to_fast, FastParameter};
use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use crate::types::PyFastExecutionResult;

/// Type for a single direct connection (not pooled)
type SingleConnectionType = Client<tokio_util::compat::Compat<TcpStream>>;

/// A single dedicated connection (not pooled) for transaction support.
/// This holds one physical database connection that persists across queries,
/// allowing SQL Server transactions (BEGIN/COMMIT/ROLLBACK) to work correctly.
#[pyclass(name = "SingleConnection")]
pub struct PySingleConnection {
    conn: Arc<AsyncMutex<Option<SingleConnectionType>>>,
    config: Arc<Config>,
    _ssl_config: Option<PySslConfig>,
    connected: Arc<SyncMutex<bool>>,
}

impl PySingleConnection {
    /// For queries that return rows (SELECT statements)
    async fn execute_query_async_gil_free(
        conn: &mut SingleConnectionType,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<Vec<Row>> {
        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
            .iter()
            .map(|p| p as &dyn tiberius::ToSql)
            .collect();

        let stream = conn
            .query(query, &tiberius_params)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;

        stream
            .into_first_result()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))
    }

    /// For commands that don't return rows (INSERT/UPDATE/DELETE/DDL)
    async fn execute_command_async_gil_free(
        conn: &mut SingleConnectionType,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<u64> {
        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
            .iter()
            .map(|p| p as &dyn tiberius::ToSql)
            .collect();

        let affected = conn
            .execute(query, &tiberius_params)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Command execution failed: {}", e)))?
            .total();

        Ok(affected)
    }
}

#[pymethods]
impl PySingleConnection {
    #[new]
    #[pyo3(signature = (connection_string = None, pool_config = None, ssl_config = None, server = None, database = None, username = None, password = None, application_intent = None, port = None, instance_name = None, application_name = None))]
    pub fn new(
        connection_string: Option<String>,
        pool_config: Option<PyPoolConfig>,
        ssl_config: Option<PySslConfig>,
        server: Option<String>,
        database: Option<String>,
        username: Option<String>,
        password: Option<String>,
        application_intent: Option<String>,
        port: Option<u16>,
        instance_name: Option<String>,
        application_name: Option<String>,
    ) -> PyResult<Self> {
        let config = if let Some(conn_str) = connection_string {
            Config::from_ado_string(&conn_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?
        } else if let Some(srv) = server {
            let mut config = Config::new();
            config.host(&srv);
            if let Some(db) = database {
                config.database(&db);
            }
            if let Some(user) = username {
                let pwd = password.ok_or_else(|| {
                    PyValueError::new_err("password is required when username is provided")
                })?;
                config.authentication(AuthMethod::sql_server(&user, &pwd));
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
                        )))
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

        Ok(PySingleConnection {
            conn: Arc::new(AsyncMutex::new(None)),
            config: Arc::new(config),
            _ssl_config: ssl_config,
            connected: Arc::new(SyncMutex::new(false)),
        })
    }

    /// Execute a SQL query that returns rows (SELECT statements)
    /// Returns rows as PyFastExecutionResult
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
        let connected = Arc::clone(&self.connected);

        future_into_py(py, async move {
            // Ensure connection is established
            {
                let mut conn_guard = conn.lock().await;
                if conn_guard.is_none() {
                    // Create a direct TCP connection to the server
                    let host = "localhost".to_string();
                    let port = 1433u16;
                    
                    let tcp_stream = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect to server: {}", e)))?;
                    
                    let compat_stream = tcp_stream.compat();
                    let new_conn: SingleConnectionType = Client::connect((*config).clone(), compat_stream)
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect to database: {}", e)))?;
                    *conn_guard = Some(new_conn);
                }
            }

            // Mark as connected
            {
                let mut connected_guard = connected.lock();
                *connected_guard = true;
            }

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
                    .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?
                    .into_first_result()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))?;
                
                drop(conn_guard); // Release lock after consuming all results
                result
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let fast_result = PyFastExecutionResult::with_rows(execution_result, py)?;
                let py_result = Py::new(py, fast_result)?;
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
        let connected = Arc::clone(&self.connected);

        future_into_py(py, async move {
            // Ensure connection is established
            {
                let mut conn_guard = conn.lock().await;
                if conn_guard.is_none() {
                    // Create a direct TCP connection to the server
                    let host = "localhost".to_string();
                    let port = 1433u16;
                    
                    let tcp_stream = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect to server: {}", e)))?;
                    
                    let compat_stream = tcp_stream.compat();
                    let new_conn: SingleConnectionType = Client::connect((*config).clone(), compat_stream)
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect to database: {}", e)))?;
                    *conn_guard = Some(new_conn);
                }
            }

            // Mark as connected
            {
                let mut connected_guard = connected.lock();
                *connected_guard = true;
            }

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
                    .map_err(|e| PyRuntimeError::new_err(format!("Command execution failed: {}", e)))?;
                
                drop(conn_guard); // Release lock
                
                result.total()
            };

            Ok(affected)
        })
    }

    /// Close the connection
    pub fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let conn = Arc::clone(&self.conn);
        let connected = Arc::clone(&self.connected);

        future_into_py(py, async move {
            {
                let mut conn_guard = conn.lock().await;
                if let Some(_c) = conn_guard.take() {
                    // Connection will be dropped and closed when it leaves scope
                }
            }

            {
                let mut connected_guard = connected.lock();
                *connected_guard = false;
            }

            Ok(())
        })
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.connected.lock()
    }
}
