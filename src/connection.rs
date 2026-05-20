use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, Row};
use tokio::sync::RwLock;

use crate::azure_auth::PyAzureCredential;
use crate::batch::{bulk_insert, execute_batch, query_batch};
use crate::helpers::wrap_query_stream;
use crate::parameter_conversion::{FastParameter, convert_parameters_to_fast, params_as_sql_refs};
use crate::pool_config::PyPoolConfig;
use crate::pool_manager::{ConnectionPool, ensure_pool_initialized_with_auth};
use crate::ssl_config::PySslConfig;
use crate::types::{create_connection_error, create_sql_error};

/// Bundles the four cloned handles needed for async pool operations.
struct ConnectionHandles {
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    azure_credential: Option<PyAzureCredential>,
}

impl ConnectionHandles {
    fn ensure_connected(&self) -> impl std::future::Future<Output = PyResult<ConnectionPool>> + '_ {
        ensure_pool_initialized_with_auth(
            self.pool.clone(),
            self.config.clone(),
            &self.pool_config,
            self.azure_credential.clone(),
        )
    }
}

#[pyclass(name = "Connection")]
pub struct PyConnection {
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>,
    azure_credential: Option<PyAzureCredential>,
}

impl PyConnection {
    /// Clone the four fields needed for async pool operations into a single struct.
    fn clone_handles(&self) -> ConnectionHandles {
        ConnectionHandles {
            pool: Arc::clone(&self.pool),
            config: Arc::clone(&self.config),
            pool_config: self.pool_config.clone(),
            azure_credential: self.azure_credential.clone(),
        }
    }

    /// Acquire a connection from the pool with standardized error mapping.
    async fn get_pool_connection(
        pool: &ConnectionPool,
    ) -> PyResult<bb8::PooledConnection<'_, crate::pool_manager::AzureConnectionManager>> {
        pool.get().await.map_err(|e| match e {
            bb8::RunError::TimedOut => {
                create_connection_error(
                    "Connection pool timeout - all connections are busy. \
                     Try reducing concurrent requests or increasing pool size.",
                )
            }
            bb8::RunError::User(e) => {
                create_connection_error(format!("Failed to get connection from pool: {}", e))
            }
        })
    }

    /// For queries that return rows (SELECT statements)
    #[inline]
    async fn execute_query_async_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<Vec<Row>> {
        Self::execute_query_internal_gil_free(pool, query, parameters).await
    }

    /// For commands that don't return rows (INSERT/UPDATE/DELETE/DDL)
    #[inline]
    async fn execute_command_async_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<u64> {
        Self::execute_command_internal_gil_free(pool, query, parameters).await
    }

    /// For queries that return rows (SELECT statements) but need to be raw non-prepared statements
    #[inline]
    async fn execute_simple_query_async_gil_free(
        pool: &ConnectionPool,
        query: &str,
    ) -> PyResult<Vec<Row>> {
        Self::execute_simple_query_internal_gil_free(pool, query).await
    }

    /// Uses query() method to get rows
    #[inline]
    async fn execute_query_internal_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<Vec<Row>> {
        let mut conn = Self::get_pool_connection(pool).await?;

        let tiberius_params = params_as_sql_refs(parameters);

        let stream = conn
            .query(query, &tiberius_params)
            .await
            .map_err(|e| create_sql_error(e, "Query execution failed"))?;

        let result = stream
            .into_first_result()
            .await
            .map_err(|e| create_sql_error(e, "Failed to get results"))?;

        drop(conn);
        Ok(result)
    }

    /// Uses simple_query() method to execute raw SQL
    #[inline]
    async fn execute_simple_query_internal_gil_free(
        pool: &ConnectionPool,
        query: &str,
    ) -> PyResult<Vec<Row>> {
        let mut conn = Self::get_pool_connection(pool).await?;

        let stream = conn
            .simple_query(query)
            .await
            .map_err(|e| create_sql_error(e, "Query execution failed"))?;

        let result = stream
            .into_first_result()
            .await
            .map_err(|e| create_sql_error(e, "Failed to get results"))?;

        drop(conn);
        Ok(result)
    }

    /// Uses execute() method to get affected row count
    #[inline]
    async fn execute_command_internal_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<u64> {
        let mut conn = Self::get_pool_connection(pool).await?;

        let tiberius_params = params_as_sql_refs(parameters);

        let result = conn
            .execute(query, &tiberius_params)
            .await
            .map_err(|e| create_sql_error(e, "Command execution failed"))?;

        let total_affected = result.rows_affected().iter().sum::<u64>();

        drop(conn);
        Ok(total_affected)
    }
}

#[pymethods]
impl PyConnection {
    #[new]
    #[pyo3(signature = (connection_string = None, pool_config = None, ssl_config = None, azure_credential = None, server = None, database = None, username = None, password = None, application_intent = None, port = None, instance_name = None, application_name = None))]
    pub fn new(
        connection_string: Option<String>,
        pool_config: Option<PyPoolConfig>,
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
        let config = if let Some(conn_str) = connection_string {
            // Use provided connection string
            Config::from_ado_string(&conn_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?
        } else if let Some(ref srv) = server {
            let mut config = Config::new();
            config.host(srv);
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
        if server.is_some() && username.is_none() && azure_credential.is_none() {
            return Err(PyValueError::new_err(
                "When using individual connection parameters, either username/password or azure_credential must be provided",
            ));
        }

        let pool_config = pool_config.unwrap_or_else(PyPoolConfig::default);

        Ok(PyConnection {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(config),
            pool_config,
            _ssl_config: ssl_config,
            azure_credential,
        })
    }

    /// Execute a SQL query that returns rows (SELECT statements)
    /// Returns rows as PyQueryStream
    #[pyo3(signature = (query, parameters=None))]
    pub fn query<'p>(
        &self,
        py: Python<'p>,
        query: String,
        parameters: Option<&Bound<PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let fast_parameters = convert_parameters_to_fast(parameters, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            let pool_ref = handles.ensure_connected().await?;
            let execution_result =
                Self::execute_query_async_gil_free(&pool_ref, &query, &fast_parameters).await?;
            wrap_query_stream(execution_result)
        })
    }

    /// Execute a raw (non-prepared statement) SQL query
    /// Returns rows as PyQueryStream
    #[pyo3(signature = (query))]
    pub fn simple_query<'p>(
        &self,
        py: Python<'p>,
        query: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();

        future_into_py(py, async move {
            let pool_ref = handles.ensure_connected().await?;
            let execution_result =
                Self::execute_simple_query_async_gil_free(&pool_ref, &query).await?;
            wrap_query_stream(execution_result)
        })
    }

    /// Execute a SQL command that doesn't return rows (INSERT/UPDATE/DELETE/DDL)
    /// Returns affected row count as u64
    #[pyo3(signature = (query, parameters=None))]
    pub fn execute<'p>(
        &self,
        py: Python<'p>,
        query: String,
        parameters: Option<&Bound<PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let fast_parameters = convert_parameters_to_fast(parameters, py)?;
        let handles = self.clone_handles();

        future_into_py(py, async move {
            let pool_ref = handles.ensure_connected().await?;
            let affected_count =
                Self::execute_command_async_gil_free(&pool_ref, &query, &fast_parameters).await?;
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(affected_count.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Check if connected to the database
    pub fn is_connected<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();

        future_into_py(py, async move {
            let is_connected = pool.read().await.is_some();
            Ok(is_connected)
        })
    }

    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let max_size = self.pool_config.max_size;
        let min_idle = self.pool_config.min_idle;

        future_into_py(py, async move {
            let (is_connected, connections, idle_connections) = {
                let pool_guard = pool.read().await;
                if let Some(pool_ref) = pool_guard.as_ref() {
                    let state = pool_ref.state();
                    (true, state.connections, state.idle_connections)
                } else {
                    (false, 0u32, 0u32)
                }
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let dict = pyo3::types::PyDict::new(py);

                dict.set_item("connected", is_connected)?;
                dict.set_item("connections", connections)?;
                dict.set_item("idle_connections", idle_connections)?;
                dict.set_item(
                    "active_connections",
                    connections.saturating_sub(idle_connections),
                )?;
                dict.set_item("max_size", max_size)?;
                dict.set_item("min_idle", min_idle)?;

                Ok(dict.into_any().unbind())
            })
        })
    }

    /// Enter context manager
    pub fn __aenter__<'p>(slf: &'p Bound<Self>, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let borrowed = slf.borrow();
        let handles = borrowed.clone_handles();

        future_into_py(py, async move {
            let _ = handles.ensure_connected().await?;
            Ok(())
        })
    }

    /// Exit context manager
    pub fn __aexit__<'p>(
        &self,
        py: Python<'p>,
        _exc_type: Option<Bound<PyAny>>,
        _exc_value: Option<Bound<PyAny>>,
        _traceback: Option<Bound<PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let pool = Arc::clone(&self.pool);
        future_into_py(py, async move {
            *pool.write().await = None;
            Ok(())
        })
    }

    /// Explicitly establish a connection (initialize the pool if not already connected)
    pub fn connect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();

        future_into_py(py, async move {
            let _ = handles.ensure_connected().await?;
            Ok(true)
        })
    }

    /// Explicitly close the connection (drop the pool)
    pub fn disconnect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = Arc::clone(&self.pool);

        future_into_py(py, async move {
            let mut pool_guard = pool.write().await;
            let had_pool = pool_guard.is_some();
            *pool_guard = None;
            Ok(had_pool)
        })
    }

    #[pyo3(signature = (queries))]
    pub fn query_batch<'p>(
        &self,
        py: Python<'p>,
        queries: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();
        query_batch(
            handles.pool,
            handles.config,
            handles.pool_config,
            handles.azure_credential,
            py,
            queries,
        )
    }

    pub fn bulk_insert<'p>(
        &self,
        py: Python<'p>,
        table_name: String,
        columns: Vec<String>,
        data_rows: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();
        bulk_insert(
            handles.pool,
            handles.config,
            handles.pool_config,
            handles.azure_credential,
            py,
            table_name,
            columns,
            data_rows,
        )
    }

    pub fn execute_batch<'p>(
        &self,
        py: Python<'p>,
        commands: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let handles = self.clone_handles();
        execute_batch(handles.config, handles.azure_credential, py, commands)
    }
}
