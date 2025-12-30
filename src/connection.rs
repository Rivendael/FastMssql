use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use parking_lot::Mutex;
use tiberius::{AuthMethod, Config, Row};

use crate::batch::{bulk_insert, execute_batch, query_batch};
use crate::parameter_conversion::{convert_parameters_to_fast, FastParameter};
use crate::pool_config::PyPoolConfig;
use crate::pool_manager::{ensure_pool_initialized, ConnectionPool};
use crate::ssl_config::PySslConfig;
use crate::types::PyFastExecutionResult;

#[pyclass(name = "Connection")]
pub struct PyConnection {
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Arc<Config>,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>,
}

impl PyConnection {
    /// For queries that return rows (SELECT statements)
    async fn execute_query_async_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<Vec<Row>> {
        Self::execute_query_internal_gil_free(pool, query, parameters).await
    }

    /// For commands that don't return rows (INSERT/UPDATE/DELETE/DDL)
    async fn execute_command_async_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<u64> {
        Self::execute_command_internal_gil_free(pool, query, parameters).await
    }

    /// Uses query() method to get rows
    async fn execute_query_internal_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<Vec<Row>> {
        let mut conn = pool.get().await
            .map_err(|e| {
                match e {
                    _ if e.to_string().contains("timed out") => {
                        PyRuntimeError::new_err("Connection pool timeout - all connections are busy. Try reducing concurrent requests or increasing pool size.")
                    },
                    _ => PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e))
                }
            })?;

        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
            .iter()
            .map(|p| p as &dyn tiberius::ToSql)
            .collect();

        let stream = conn
            .query(query, &tiberius_params)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;

        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))?;

        Ok(rows)
    }

    /// Uses execute() method to get affected row count
    async fn execute_command_internal_gil_free(
        pool: &ConnectionPool,
        query: &str,
        parameters: &[FastParameter],
    ) -> PyResult<u64> {
        let mut conn = pool.get().await
            .map_err(|e| {
                match e {
                    _ if e.to_string().contains("timed out") => {
                        PyRuntimeError::new_err("Connection pool timeout - all connections are busy. Try reducing concurrent requests or increasing pool size.")
                    },
                    _ => PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e))
                }
            })?;

        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 16]> = parameters
            .iter()
            .map(|p| p as &dyn tiberius::ToSql)
            .collect();

        let result = conn
            .execute(query, &tiberius_params)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Command execution failed: {}", e)))?;

        let total_affected: u64 = result.rows_affected().iter().sum();
        Ok(total_affected)
    }
}

#[pymethods]
impl PyConnection {
    #[new]
    #[pyo3(signature = (connection_string = None, pool_config = None, ssl_config = None, server = None, database = None, username = None, password = None, trusted_connection = None))]
    pub fn new(
        connection_string: Option<String>,
        pool_config: Option<PyPoolConfig>,
        ssl_config: Option<PySslConfig>,
        server: Option<String>,
        database: Option<String>,
        username: Option<String>,
        password: Option<String>,
        trusted_connection: Option<bool>,
    ) -> PyResult<Self> {
        let mut config = if let Some(conn_str) = connection_string {
            // Use provided connection string
            Config::from_ado_string(&conn_str)
                .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?
        } else if let Some(srv) = server {
            // Build config from individual parameters
            let mut config = Config::new();
            config.host(&srv);

            if let Some(db) = database {
                config.database(&db);
            }

            if let Some(user) = username {
                config.authentication(AuthMethod::sql_server(&user, &password.unwrap_or_default()));
            } else if trusted_connection.unwrap_or(true) {
                return Err(PyValueError::new_err(
                    "Windows authentication is not supported. Please provide username and password for SQL Server authentication."
                ));
            }

            config
        } else {
            return Err(PyValueError::new_err(
                "Either connection_string or server must be provided",
            ));
        };

        // Apply SSL configuration if provided
        if let Some(ref ssl_cfg) = ssl_config {
            ssl_cfg.apply_to_config(&mut config);
        }

        let pool_config = pool_config.unwrap_or_else(PyPoolConfig::default);

        Ok(PyConnection {
            pool: Arc::new(Mutex::new(None)),
            config: Arc::new(config),
            pool_config,
            _ssl_config: ssl_config,
        })
    }

    /// Execute a SQL query that returns rows (SELECT statements)
    /// Returns rows as PyFastExecutionResult
    #[pyo3(signature = (query, parameters=None))]
    pub fn query<'p>(
        &self,
        py: Python<'p>,
        query: String,
        parameters: Option<&Bound<PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let fast_parameters = convert_parameters_to_fast(parameters, py)?;

        let pool = Arc::clone(&self.pool);
        let config = Arc::clone(&self.config);
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_ref = ensure_pool_initialized(pool, config, &pool_config).await?;

            let execution_result =
                Self::execute_query_async_gil_free(&pool_ref, &query, &fast_parameters).await?;

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let fast_result = PyFastExecutionResult::with_rows(execution_result, py)?;
                let py_result = Py::new(py, fast_result)?;
                Ok(py_result.into_any())
            })
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

        let pool = Arc::clone(&self.pool);
        let config = Arc::clone(&self.config);
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_ref = ensure_pool_initialized(pool, config, &pool_config).await?;

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
            let pool_guard = pool.lock();
            let is_connected = pool_guard.is_some();
            Ok(is_connected)
        })
    }

    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_guard = pool.lock();
            if let Some(ref pool_ref) = *pool_guard {
                let state = pool_ref.state();
                Ok((
                    true, // connected
                    state.connections,
                    state.idle_connections,
                    pool_config.max_size,
                    pool_config.min_idle,
                ))
            } else {
                Ok((false, 0u32, 0u32, 0u32, None))
            }
        })
    }

    /// Enter context manager (async version)
    pub fn __aenter__<'p>(slf: &'p Bound<Self>, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let borrowed = slf.borrow();
        let pool = Arc::clone(&borrowed.pool);
        let config = Arc::clone(&borrowed.config);
        let pool_config = borrowed.pool_config.clone();

        future_into_py(py, async move {
            let is_connected = {
                let pool_guard = pool.lock();
                pool_guard.is_some()
            };
            
            if is_connected {
                return Ok(());
            }

            let _ = ensure_pool_initialized(pool, config, &pool_config).await?;
            Ok(())
        })
    }

    /// Exit context manager (async version)
    pub fn __aexit__<'p>(
        &self,
        py: Python<'p>,
        _exc_type: Option<Bound<PyAny>>,
        _exc_value: Option<Bound<PyAny>>,
        _traceback: Option<Bound<PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();

        future_into_py(py, async move {
            let mut pool_guard = pool.lock();
            if let Some(pool_ref) = pool_guard.take() {
                drop(pool_ref);
            }
            Ok(())
        })
    }

    /// Explicitly establish a connection (initialize the pool if not already connected)
    pub fn connect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = Arc::clone(&self.pool);
        let config = Arc::clone(&self.config);
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let is_connected = {
                let pool_guard = pool.lock();
                pool_guard.is_some()
            };
            
            if is_connected {
                return Ok(true);
            }

            let _ = ensure_pool_initialized(pool, config, &pool_config).await?;
            Ok(true)
        })
    }

    /// Explicitly close the connection (drop the pool)
    pub fn disconnect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();

        future_into_py(py, async move {
            let mut pool_guard = pool.lock();
            if let Some(pool_ref) = pool_guard.take() {
                drop(pool_ref);
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    #[pyo3(signature = (queries))]
    pub fn query_batch<'p>(
        &self,
        py: Python<'p>,
        queries: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        query_batch(
            Arc::clone(&self.pool),
            Arc::clone(&self.config),
            self.pool_config.clone(),
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
        bulk_insert(
            Arc::clone(&self.pool),
            Arc::clone(&self.config),
            self.pool_config.clone(),
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
        execute_batch(
            Arc::clone(&self.pool),
            Arc::clone(&self.config),
            self.pool_config.clone(),
            py,
            commands,
        )
    }
}
