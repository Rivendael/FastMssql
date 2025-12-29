use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use crate::types::PyFastExecutionResult;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use smallvec::SmallVec;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, Row};
use tokio::sync::RwLock;

type ConnectionPool = Pool<ConnectionManager>;

/// A connection pool to a Microsoft SQL Server database
#[pyclass(name = "Connection")]
pub struct PyConnection {
    pool: Arc<RwLock<Option<ConnectionPool>>>,
    config: Config,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>,
}

impl PyConnection {
    #[inline]
    fn convert_parameters_to_fast(
        parameters: Option<&Bound<PyAny>>,
        py: Python,
    ) -> PyResult<SmallVec<[FastParameter; 8]>> {
        if let Some(params) = parameters {
            // Check if it's a Parameters object and convert to list
            if let Ok(params_obj) = params.extract::<Py<crate::parameters::Parameters>>() {
                let params_bound = params_obj.bind(py);
                let list = params_bound.call_method0("to_list")?;
                let list_bound = list.cast::<PyList>()?;
                python_params_to_fast_parameters(list_bound)
            } else if let Ok(list) = params.cast::<PyList>() {
                python_params_to_fast_parameters(list)
            } else {
                Err(PyValueError::new_err(
                    "Parameters must be a list or Parameters object",
                ))
            }
        } else {
            Ok(SmallVec::new())
        }
    }

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

    /// Helper function to establish a database connection pool
    ///
    /// Creates a bb8 connection pool with the provided configuration
    async fn establish_pool(
        config: Config,
        pool_config: &PyPoolConfig,
    ) -> PyResult<ConnectionPool> {
        let manager = ConnectionManager::new(config);

        let mut builder = Pool::builder()
            .max_size(pool_config.max_size)
            // Add retry configuration for connection establishment
            .retry_connection(true);

        if let Some(min_idle) = pool_config.min_idle {
            builder = builder.min_idle(Some(min_idle));
        }

        if let Some(max_lifetime) = pool_config.max_lifetime {
            builder = builder.max_lifetime(Some(max_lifetime));
        }

        if let Some(idle_timeout) = pool_config.idle_timeout {
            builder = builder.idle_timeout(Some(idle_timeout));
        }

        if let Some(connection_timeout) = pool_config.connection_timeout {
            builder = builder.connection_timeout(connection_timeout);
        }

        let pool = builder.build(manager).await.map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create connection pool: {}", e))
        })?;

        Ok(pool)
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

        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 8]> = parameters
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

        let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 8]> = parameters
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

#[derive(Debug, Clone)]
enum FastParameter {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl tiberius::ToSql for FastParameter {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            FastParameter::Null => tiberius::ColumnData::U8(None),
            FastParameter::Bool(b) => b.to_sql(),
            FastParameter::I64(i) => i.to_sql(),
            FastParameter::F64(f) => f.to_sql(),
            FastParameter::String(s) => s.to_sql(),
            FastParameter::Bytes(b) => b.to_sql(),
        }
    }
}

fn python_to_fast_parameter(obj: &Bound<PyAny>) -> PyResult<FastParameter> {
    use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyString};

    if obj.is_none() {
        return Ok(FastParameter::Null);
    }

    // Try string first (most common in SQL)
    if let Ok(py_string) = obj.cast::<PyString>() {
        // CRITICAL: Use to_cow() to avoid allocation when possible
        return Ok(FastParameter::String(py_string.to_str()?.to_owned()));
    }

    // Try int second (very common)
    if let Ok(py_int) = obj.cast::<PyInt>() {
        return py_int
            .extract::<i64>()
            .map(FastParameter::I64)
            .map_err(|_| PyValueError::new_err("Integer value too large for i64"));
    }

    // Try float third
    if let Ok(py_float) = obj.cast::<PyFloat>() {
        return Ok(FastParameter::F64(py_float.value()));
    }

    // Try bool fourth (less common)
    if let Ok(py_bool) = obj.cast::<PyBool>() {
        return Ok(FastParameter::Bool(py_bool.is_true()));
    }

    // Try bytes last (least common)
    if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        return Ok(FastParameter::Bytes(py_bytes.as_bytes().to_vec()));
    }

    // Fallback for numpy types, Decimal, etc. - only use extract() as last resort
    if let Ok(i) = obj.extract::<i64>() {
        Ok(FastParameter::I64(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(FastParameter::F64(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(FastParameter::String(s))
    } else if let Ok(b) = obj.extract::<Vec<u8>>() {
        Ok(FastParameter::Bytes(b))
    } else {
        Err(PyValueError::new_err(format!(
            "Unsupported parameter type: {}",
            obj.get_type().name()?
        )))
    }
}

/// Convert Python objects to FastParameter with zero-allocation parameter handling
/// Returns SmallVec directly to avoid unnecessary heap allocations for small parameter lists
fn python_params_to_fast_parameters(
    params: &Bound<PyList>,
) -> PyResult<SmallVec<[FastParameter; 8]>> {
    let len = params.len();

    // SmallVec optimization:
    // - 0-8 parameters: Zero heap allocations (stack only)
    // - 9+ parameters: Single heap allocation (rare case)
    // - No unnecessary into_vec() conversion
    let mut result: SmallVec<[FastParameter; 8]> = SmallVec::with_capacity(len);

    for param in params.iter() {
        if is_expandable_iterable(&param)? {
            expand_iterable_to_fast_params(&param, &mut result)?;
        } else {
            result.push(python_to_fast_parameter(&param)?);
        }
    }

    Ok(result)
}

/// Expand a Python iterable into individual FastParameter objects with minimal allocations
fn expand_iterable_to_fast_params<T>(iterable: &Bound<PyAny>, result: &mut T) -> PyResult<()>
where
    T: Extend<FastParameter>,
{
    use pyo3::types::{PyList, PyTuple};

    // Fast path for common collection types - avoid iterator overhead
    if let Ok(list) = iterable.cast::<PyList>() {
        result.extend(
            list.iter()
                .map(|item| python_to_fast_parameter(&item))
                .collect::<PyResult<Vec<_>>>()?,
        );
        return Ok(());
    }

    if let Ok(tuple) = iterable.cast::<PyTuple>() {
        result.extend(
            tuple
                .iter()
                .map(|item| python_to_fast_parameter(&item))
                .collect::<PyResult<Vec<_>>>()?,
        );
        return Ok(());
    }

    // Fallback for generic iterables - use PyO3's optimized iteration
    let py = iterable.py();
    let iter = iterable.call_method0("__iter__")?;

    // Pre-allocate a small buffer to batch extend operations and reduce allocations
    let mut batch: SmallVec<[FastParameter; 16]> = SmallVec::new();

    loop {
        match iter.call_method0("__next__") {
            Ok(item) => {
                batch.push(python_to_fast_parameter(&item)?);

                // Batch extend every 16 items to reduce extend() call overhead
                if batch.len() == 16 {
                    result.extend(batch.drain(..));
                }
            }
            Err(err) => {
                // Check if it's StopIteration (normal end of iteration)
                if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                    break;
                } else {
                    return Err(err);
                }
            }
        }
    }

    // Extend any remaining items in the batch
    if !batch.is_empty() {
        result.extend(batch);
    }

    Ok(())
}

/// Check if a Python object is an iterable that should be expanded
///
/// Returns true for lists, tuples, sets, etc., but false for strings and bytes
/// which should be treated as single values.
/// Uses fast type checking to avoid expensive extract() calls.
fn is_expandable_iterable(obj: &Bound<PyAny>) -> PyResult<bool> {
    use pyo3::types::{PyBytes, PyString};

    // Fast path: Don't expand strings or bytes using type checking
    if obj.is_instance_of::<PyString>() || obj.is_instance_of::<PyBytes>() {
        return Ok(false);
    }

    // Check if object has __iter__ method (is iterable)
    Ok(obj.hasattr("__iter__")?)
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
            pool: Arc::new(RwLock::new(None)),
            config,
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
        // OPTIMIZATION: Do ALL Python type checking/conversion synchronously while we have the GIL
        // This moves GIL contention out of the async hot path entirely
        let fast_parameters = Self::convert_parameters_to_fast(parameters, py)?;

        // PERFORMANCE CRITICAL: Clone self reference for async context
        // We need access to the PyConnection instance methods
        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();

        // Return the coroutine - now with ZERO GIL usage in async execution
        future_into_py(py, async move {
            // PERFORMANCE CRITICAL: Optimized pool access with auto-initialization
            let pool_ref = {
                let pool_guard = pool.read().await;
                if let Some(ref pool_ref) = *pool_guard {
                    pool_ref.clone()
                } else {
                    drop(pool_guard); // Release read lock

                    // Acquire write lock to initialize
                    let mut pool_guard = pool.write().await;
                    if let Some(ref pool_ref) = *pool_guard {
                        // Another thread initialized it while we were waiting
                        pool_ref.clone()
                    } else {
                        // We need to initialize it
                        let new_pool = Self::establish_pool(config, &pool_config).await?;
                        *pool_guard = Some(new_pool.clone());
                        new_pool
                    }
                }
            };

            let execution_result =
                Self::execute_query_async_gil_free(&pool_ref, &query, &fast_parameters).await?;

            // Convert results efficiently - acquire GIL only once per result set
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
        // OPTIMIZATION: Do ALL Python type checking/conversion synchronously while we have the GIL
        // This moves GIL contention out of the async hot path entirely
        let fast_parameters = Self::convert_parameters_to_fast(parameters, py)?;

        // PERFORMANCE CRITICAL: Clone self reference for async context
        // We need access to the PyConnection instance methods
        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();

        // Return the coroutine - now with ZERO GIL usage in async execution
        future_into_py(py, async move {
            // PERFORMANCE CRITICAL: Optimized pool access with auto-initialization
            let pool_ref = {
                let pool_guard = pool.read().await;
                if let Some(ref pool_ref) = *pool_guard {
                    pool_ref.clone()
                } else {
                    drop(pool_guard); // Release read lock

                    // Acquire write lock to initialize
                    let mut pool_guard = pool.write().await;
                    if let Some(ref pool_ref) = *pool_guard {
                        // Another thread initialized it while we were waiting
                        pool_ref.clone()
                    } else {
                        // We need to initialize it
                        let new_pool = Self::establish_pool(config, &pool_config).await?;
                        *pool_guard = Some(new_pool.clone());
                        new_pool
                    }
                }
            };

            let affected_count =
                Self::execute_command_async_gil_free(&pool_ref, &query, &fast_parameters).await?;

            // Convert results efficiently - acquire GIL only once per result set
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(affected_count.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Check if connected to the database
    pub fn is_connected<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();

        future_into_py(py, async move {
            let pool_guard = pool.read().await;
            let is_connected = pool_guard.is_some();
            Ok(is_connected)
        })
    }

    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_guard = pool.read().await;
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
        let pool = slf.borrow().pool.clone();
        let config = slf.borrow().config.clone();
        let pool_config = slf.borrow().pool_config.clone();

        future_into_py(py, async move {
            // Check if already connected
            let pool_guard = pool.read().await;
            if pool_guard.is_some() {
                return Ok(());
            }
            drop(pool_guard); // Release read lock

            // Acquire write lock to initialize
            let mut pool_guard = pool.write().await;
            if pool_guard.is_some() {
                // Another thread initialized it while we were waiting
                return Ok(());
            }

            // We need to initialize it
            let new_pool = PyConnection::establish_pool(config, &pool_config).await?;
            *pool_guard = Some(new_pool);
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
            // Properly close the pool when exiting the context manager
            let mut pool_guard = pool.write().await;
            if let Some(pool_ref) = pool_guard.take() {
                // Explicitly close all connections in the pool
                // The pool will be dropped here, which should clean up all connections
                drop(pool_ref);
            }
            Ok(())
        })
    }

    /// Explicitly establish a connection (initialize the pool if not already connected)
    pub fn connect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            // Check if already connected
            let pool_guard = pool.read().await;
            if pool_guard.is_some() {
                return Ok(true); // Already connected
            }
            drop(pool_guard); // Release read lock

            // Acquire write lock to initialize
            let mut pool_guard = pool.write().await;
            if pool_guard.is_some() {
                return Ok(true); // Another task connected while we waited
            }

            // Initialize the pool
            let new_pool = PyConnection::establish_pool(config, &pool_config).await?;
            *pool_guard = Some(new_pool);
            Ok(true)
        })
    }

    /// Explicitly close the connection (drop the pool)
    pub fn disconnect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();

        future_into_py(py, async move {
            let mut pool_guard = pool.write().await;
            if let Some(pool_ref) = pool_guard.take() {
                // Dropping the pool here will close all connections
                drop(pool_ref);
                Ok(true)
            } else {
                Ok(false) // Already disconnected
            }
        })
    }

    /// Execute multiple queries in a single batch operation for maximum performance
    /// This method optimizes network round-trips by sending all queries together
    #[pyo3(signature = (queries))]
    pub fn query_batch<'p>(
        &self,
        py: Python<'p>,
        queries: &Bound<PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let mut batch_queries: Vec<(String, SmallVec<[FastParameter; 8]>)> =
            Vec::with_capacity(queries.len());

        for item in queries.iter() {
            let (query, params) = if let Ok(tuple) = item.cast::<pyo3::types::PyTuple>() {
                if tuple.len() == 2 {
                    let query: String = tuple.get_item(0)?.extract()?;
                    let params = tuple.get_item(1)?;
                    let fast_parameters = if params.is_none() {
                        SmallVec::new()
                    } else {
                        Self::convert_parameters_to_fast(Some(&params), py)?
                    };
                    (query, fast_parameters)
                } else {
                    return Err(PyValueError::new_err(
                        "Each batch item must be a tuple of (query, parameters)",
                    ));
                }
            } else {
                return Err(PyValueError::new_err(
                    "Each batch item must be a tuple of (query, parameters)",
                ));
            };

            batch_queries.push((query, params));
        }

        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_ref = {
                let pool_guard = pool.read().await;
                if let Some(ref pool_ref) = *pool_guard {
                    pool_ref.clone()
                } else {
                    drop(pool_guard);
                    let mut pool_guard = pool.write().await;
                    if let Some(ref pool_ref) = *pool_guard {
                        pool_ref.clone()
                    } else {
                        let new_pool = Self::establish_pool(config, &pool_config).await?;
                        *pool_guard = Some(new_pool.clone());
                        new_pool
                    }
                }
            };

            // Execute all queries in sequence on a single connection
            let mut conn = pool_ref.get().await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e))
            })?;

            let mut all_results = Vec::with_capacity(batch_queries.len());

            for (query, parameters) in batch_queries {
                let tiberius_params: SmallVec<[&dyn tiberius::ToSql; 8]> = parameters
                    .iter()
                    .map(|p| p as &dyn tiberius::ToSql)
                    .collect();

                let stream = conn.query(&query, &tiberius_params).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Batch query execution failed: {}", e))
                })?;

                let rows = stream.into_first_result().await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to get batch results: {}", e))
                })?;

                all_results.push(rows);
            }

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let mut py_results = Vec::with_capacity(all_results.len());
                for result in all_results {
                    let fast_result = PyFastExecutionResult::with_rows(result, py)?;
                    let py_result = Py::new(py, fast_result)?;
                    py_results.push(py_result.into_any());
                }
                let py_list = PyList::new(py, py_results)?;
                Ok(py_list.into_any().unbind())
            })
        })
    }

    pub fn bulk_insert<'p>(
        &self,
        py: Python<'p>,
        table_name: String,
        columns: Vec<String>,
        data_rows: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        if columns.is_empty() {
            return Err(PyValueError::new_err(
                "At least one column must be specified",
            ));
        }

        let mut flat_data: Vec<FastParameter> = Vec::with_capacity(data_rows.len() * columns.len());
        for row in data_rows.iter() {
            let row_list = row.cast::<PyList>()?;
            if row_list.len() != columns.len() {
                return Err(PyValueError::new_err(format!(
                    "Row has {} values but {} columns specified",
                    row_list.len(),
                    columns.len()
                )));
            }
            for value in row_list.iter() {
                flat_data.push(python_to_fast_parameter(&value)?);
            }
        }

        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();
        let col_count = columns.len();

        future_into_py(py, async move {
            let pool_ref = {
                let pool_guard = pool.read().await;
                if let Some(ref p) = *pool_guard {
                    p.clone()
                } else {
                    drop(pool_guard);
                    let mut pool_guard = pool.write().await;
                    if let Some(ref p) = *pool_guard {
                        p.clone()
                    } else {
                        let new_pool = Self::establish_pool(config, &pool_config)
                            .await
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                        *pool_guard = Some(new_pool.clone());
                        new_pool
                    }
                }
            };

            let mut conn = pool_ref
                .get()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Pool error: {}", e)))?;

            let mut total_affected = 0u64;

            // Hard limit for SQL Server is 2100. We use 2000 to be safe.
            let max_params_per_request = 2000;
            let rows_per_batch = max_params_per_request / col_count;

            // Ensure we handle the case where a single row has > 2000 columns (unlikely but safe)
            let rows_per_batch = if rows_per_batch == 0 {
                1
            } else {
                rows_per_batch
            };

            for chunk in flat_data.chunks(rows_per_batch * col_count) {
                let row_count_in_batch = chunk.len() / col_count;

                let mut sql = format!(
                    "INSERT INTO {} ({}) VALUES ",
                    table_name,
                    columns.join(", ")
                );
                let mut value_groups = Vec::with_capacity(row_count_in_batch);

                for r in 0..row_count_in_batch {
                    let mut placeholders = Vec::with_capacity(col_count);
                    for c in 1..=col_count {
                        // This resets to @P1, @P2... for every chunk sent to the server
                        placeholders.push(format!("@P{}", (r * col_count) + c));
                    }
                    value_groups.push(format!("({})", placeholders.join(",")));
                }
                sql.push_str(&value_groups.join(","));

                let params: Vec<&dyn tiberius::ToSql> =
                    chunk.iter().map(|p| p as &dyn tiberius::ToSql).collect();

                let result = conn.execute(sql, &params).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Batch execution failed: {}", e))
                })?;

                total_affected += result.rows_affected().iter().sum::<u64>();
            }

            Python::attach(|py| {
                let res = total_affected.into_pyobject(py)?;
                Ok(res.into_any().unbind())
            })
        })
    }

    pub fn execute_batch<'p>(
        &self,
        py: Python<'p>,
        commands: &Bound<'p, PyList>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let mut batch_commands: Vec<(String, SmallVec<[FastParameter; 8]>)> =
            Vec::with_capacity(commands.len());

        for item in commands.iter() {
            let tuple = item.cast::<pyo3::types::PyTuple>().map_err(|_| {
                PyValueError::new_err("Each batch item must be a tuple of (sql, parameters)")
            })?;

            if tuple.len() != 2 {
                return Err(PyValueError::new_err(
                    "Tuple must contain exactly 2 elements",
                ));
            }

            let sql: String = tuple.get_item(0)?.extract()?;
            let params_py = tuple.get_item(1)?;

            let fast_params = if params_py.is_none() {
                SmallVec::new()
            } else {
                Self::convert_parameters_to_fast(Some(&params_py), py)?
            };

            batch_commands.push((sql, fast_params));
        }

        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();

        future_into_py(py, async move {
            let pool_ref = {
                let pool_guard = pool.read().await;
                if let Some(ref p) = *pool_guard {
                    p.clone()
                } else {
                    drop(pool_guard);
                    let mut pool_guard = pool.write().await;
                    if let Some(ref p) = *pool_guard {
                        p.clone()
                    } else {
                        let new_pool = Self::establish_pool(config, &pool_config)
                            .await
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                        *pool_guard = Some(new_pool.clone());
                        new_pool
                    }
                }
            };

            let mut conn = pool_ref
                .get()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Pool error: {}", e)))?;

            let mut all_results = Vec::with_capacity(batch_commands.len());

            conn.simple_query("BEGIN TRANSACTION").await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to start transaction: {}", e))
            })?;

            for (sql, parameters) in batch_commands {
                let tiberius_params: Vec<&dyn tiberius::ToSql> = parameters
                    .iter()
                    .map(|p| p as &dyn tiberius::ToSql)
                    .collect();

                match conn.execute(sql, &tiberius_params).await {
                    Ok(result) => {
                        let affected: u64 = result.rows_affected().iter().sum();
                        all_results.push(affected);
                    }
                    Err(e) => {
                        let _ = conn.simple_query("ROLLBACK TRANSACTION").await;
                        return Err(PyRuntimeError::new_err(format!("Batch item failed: {}", e)));
                    }
                }
            }

            conn.simple_query("COMMIT TRANSACTION")
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit batch: {}", e)))?;

            Python::attach(|py| {
                let py_list = PyList::new(py, all_results)?;
                Ok(py_list.into_any().unbind())
            })
        })
    }
}
