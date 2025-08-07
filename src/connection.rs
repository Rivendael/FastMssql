use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3_async_runtimes::tokio::future_into_py;
use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use crate::optimized_types::PyFastExecutionResult;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config, AuthMethod, Row};
use pyo3::types::PyList;
use tokio::sync::Mutex;
use pyo3::prelude::*;
use std::sync::Arc;
use bb8::Pool;
// Memory pool for reusing Vec<FastParameter> to reduce allocations  
thread_local! {
    static PARAM_POOL: std::cell::RefCell<Vec<Vec<FastParameter>>> = 
        std::cell::RefCell::new(Vec::with_capacity(16));
}

/// Internal result type for async operations
#[derive(Debug)]
enum ExecutionResult {
    Rows(Vec<Row>),
    AffectedCount(u64),
}

type ConnectionPool = Pool<ConnectionManager>;

/// A connection pool to a Microsoft SQL Server database
#[pyclass(name = "Connection")]
pub struct PyConnection {
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Config,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>, // Prefix with underscore to silence unused warning
}

impl PyConnection {
    /// Execute database operation and return raw results - NO PYTHON CONTEXT
    async fn execute_raw_async(
        pool: Arc<Mutex<Option<ConnectionPool>>>,
        query: String,
        parameters: Vec<FastParameter>,
    ) -> PyResult<ExecutionResult> {
        let pool_guard = pool.lock().await;
        let pool_ref = pool_guard.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        Self::execute_internal_ultra_fast(pool_ref, query, parameters).await
    }



    /// Helper function to establish a database connection pool
    /// 
    /// Creates a bb8 connection pool with the provided configuration
    async fn establish_pool(config: Config, pool_config: &PyPoolConfig) -> PyResult<ConnectionPool> {
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
        
        let pool = builder
            .build(manager)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create connection pool: {}", e)))?;
        
        Ok(pool)
    }

    /// Helper function to close the connection pool
    async fn close_pool(pool: Arc<Mutex<Option<ConnectionPool>>>) {
        let mut pool_guard = pool.lock().await;
        *pool_guard = None;
    }

    /// ULTRA-FAST execution - returns raw rows for conversion in Python context
    async fn execute_internal_ultra_fast(
        pool: &ConnectionPool,
        query: String,
        parameters: Vec<FastParameter>,
    ) -> PyResult<ExecutionResult> {
        // Get connection with proper error handling for pool exhaustion
        let mut conn = pool.get().await
            .map_err(|e| {
                // Better error handling for different types of connection failures
                match e {
                    _ if e.to_string().contains("timed out") => {
                        PyRuntimeError::new_err("Connection pool timeout - all connections are busy. Try reducing concurrent requests or increasing pool size.")
                    },
                    _ => PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e))
                }
            })?;
        
        // Convert to references for tiberius - zero allocation
        let tiberius_params: Vec<&dyn tiberius::ToSql> = parameters.iter()
            .map(|p| p as &dyn tiberius::ToSql)
            .collect();
        
        // Ultra-fast SQL analysis
        let is_result_returning_query = Self::contains_result_returning_statements_ultra_fast(&query);
        
        if is_result_returning_query {
            let stream = conn.query(&query, &tiberius_params)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
            
            let rows = stream.into_first_result()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))?;
            
            Ok(ExecutionResult::Rows(rows))
        } else {
            let result = conn.execute(&query, &tiberius_params)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;

            let total_affected: u64 = result.rows_affected().iter().sum();
            Ok(ExecutionResult::AffectedCount(total_affected))
        }
    }

    /// Ultra-fast SQL analysis - optimized for hot path with zero allocations
    #[inline(always)]
    fn contains_result_returning_statements_ultra_fast(sql: &str) -> bool {
        // Zero-allocation case-insensitive search using byte comparison
        let sql_bytes = sql.as_bytes();
        let len = sql_bytes.len();
        
        if len < 6 { return false; } // Minimum length for "SELECT"
        
        // Check for SELECT at start (most common case)
        if Self::starts_with_ignore_case(sql_bytes, b"select") {
            return true;
        }
        
        // Check for WITH at start (CTE)
        if len >= 4 && Self::starts_with_ignore_case(sql_bytes, b"with") {
            return true;
        }
        
        // Check for EXEC/EXECUTE at start
        if len >= 4 && Self::starts_with_ignore_case(sql_bytes, b"exec") {
            return true;
        }
        if len >= 7 && Self::starts_with_ignore_case(sql_bytes, b"execute") {
            return true;
        }
        
        // Fast scan for " SELECT " in the middle (less common)
        for i in 1..len.saturating_sub(7) {
            if sql_bytes[i - 1] == b' ' && 
               Self::slice_eq_ignore_case(&sql_bytes[i..i+6], b"select") &&
               i + 6 < len && sql_bytes[i + 6] == b' ' {
                return true;
            }
        }
        
        false
    }
    
    /// Zero-allocation case-insensitive comparison
    #[inline(always)]
    fn starts_with_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
        if haystack.len() < needle.len() { return false; }
        Self::slice_eq_ignore_case(&haystack[..needle.len()], needle)
    }
    
    /// Zero-allocation case-insensitive slice comparison
    #[inline(always)]
    fn slice_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() { return false; }
        a.iter().zip(b.iter()).all(|(&a_byte, &b_byte)| {
            a_byte.to_ascii_lowercase() == b_byte.to_ascii_lowercase()
        })
    }
}

/// High-performance parameter conversion using enum dispatch instead of boxing
#[derive(Debug)]
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

/// Convert a Python object to FastParameter for zero-allocation parameter handling
fn python_to_fast_parameter(obj: &Bound<PyAny>) -> PyResult<FastParameter> {
    if obj.is_none() {
        Ok(FastParameter::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(FastParameter::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(FastParameter::I64(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(FastParameter::F64(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(FastParameter::String(s))
    } else if let Ok(b) = obj.extract::<Vec<u8>>() {
        Ok(FastParameter::Bytes(b))
    } else {
        Err(PyValueError::new_err(format!("Unsupported parameter type: {}", obj.get_type().name()?)))
    }
}

/// Convert Python objects to FastParameter with automatic iterable expansion
/// Pre-allocates capacity for better performance and reuses memory
fn python_params_to_fast_parameters(params: &Bound<PyList>) -> PyResult<Vec<FastParameter>> {
    let len = params.len();
    
    // Try to get a reusable vector from the pool
    let mut result = PARAM_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        pool_ref.pop().unwrap_or_else(|| Vec::with_capacity(len.max(8)))
    });
    
    // Clear and ensure capacity
    result.clear();
    if result.capacity() < len {
        result.reserve(len - result.capacity());
    }
    
    for param in params.iter() {
        if is_expandable_iterable(&param)? {
            expand_iterable_to_fast_params(&param, &mut result)?;
        } else {
            result.push(python_to_fast_parameter(&param)?);
        }
    }
    
    Ok(result)
}

/// Expand a Python iterable into individual FastParameter objects
fn expand_iterable_to_fast_params(iterable: &Bound<PyAny>, result: &mut Vec<FastParameter>) -> PyResult<()> {
    // Get the iter() method of the iterable
    let iter_method = iterable.getattr("__iter__")?;
    let iterator = iter_method.call0()?;
    
    // Iterate through the items
    loop {
        match iterator.call_method0("__next__") {
            Ok(item) => {
                result.push(python_to_fast_parameter(&item)?);
            },
            Err(_) => break, // StopIteration exception
        }
    }
    
    Ok(())
}

/// Check if a Python object is an iterable that should be expanded
/// 
/// Returns true for lists, tuples, sets, etc., but false for strings and bytes
/// which should be treated as single values.
fn is_expandable_iterable(obj: &Bound<PyAny>) -> PyResult<bool> {
    // Don't expand strings or bytes
    if obj.extract::<String>().is_ok() || obj.extract::<Vec<u8>>().is_ok() {
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
        trusted_connection: Option<bool>
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
                "Either connection_string or server must be provided"
            ));
        };

        // Apply SSL configuration if provided
        if let Some(ref ssl_cfg) = ssl_config {
            ssl_cfg.apply_to_config(&mut config);
        }
        
        let pool_config = pool_config.unwrap_or_else(PyPoolConfig::default);
        
        Ok(PyConnection {
            pool: Arc::new(Mutex::new(None)),
            config,
            pool_config,
            _ssl_config: ssl_config,
        })
    }
    
    /// Connect to the database
    pub fn connect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();
        
        future_into_py(py, async move {
            // Use a more robust check-and-create pattern to avoid race conditions
            let mut pool_guard = pool.lock().await;
            if pool_guard.is_none() {
                // Create pool while holding the lock to prevent race conditions
                let new_pool = Self::establish_pool(config, &pool_config).await?;
                *pool_guard = Some(new_pool);
            }
            drop(pool_guard); // Explicitly drop the lock
            Ok(()) // Return unit from the async function
        })
    }
    
    /// Disconnect from the database
    pub fn disconnect<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::close_pool(pool).await;
            Ok(()) // Return unit from the async function
        })
    }
    
    /// Execute a SQL statement efficiently and return appropriate results
    /// 
    /// For SELECT queries: Returns rows as PyFastExecutionResult
    /// For INSERT/UPDATE/DELETE/DDL: Returns affected row count as u64
    /// SIMPLIFIED VERSION - no caching, direct result conversion
    #[pyo3(signature = (query, parameters=None))]
    pub fn execute<'p>(&self, py: Python<'p>, query: String, parameters: Option<&Bound<PyAny>>) -> PyResult<Bound<'p, PyAny>> {
        let parameters = if let Some(params) = parameters {
            // Check if it's a Parameters object and convert to list
            if let Ok(params_obj) = params.extract::<Py<crate::parameters::Parameters>>() {
                let params_bound = params_obj.bind(py);
                let list = params_bound.call_method0("to_list")?;
                let list_bound = list.downcast::<PyList>()?;
                python_params_to_fast_parameters(list_bound)?
            } else if let Ok(list) = params.downcast::<PyList>() {
                python_params_to_fast_parameters(list)?
            } else {
                return Err(PyValueError::new_err("Parameters must be a list or Parameters object"));
            }
        } else {
            Vec::new()
        };
        
        let pool = Arc::clone(&self.pool);
        
        // Return the coroutine directly for Python to await
        future_into_py(py, async move {
            let execution_result = Self::execute_raw_async(pool, query, parameters).await?;
            
            // Convert results directly without caching
            match execution_result {
                ExecutionResult::Rows(rows) => {
                    // Convert rows to Python objects in async context
                    Python::with_gil(|py| {
                        let result = PyFastExecutionResult::with_rows(rows, py)?;
                        let py_result = Py::new(py, result)?;
                        Ok(py_result.into_any())
                    })
                },
                ExecutionResult::AffectedCount(count) => {
                    // Return affected count directly as u64
                    Python::with_gil(|py| {
                        Ok(count.into_pyobject(py)?.into_any().unbind())
                    })
                }
            }
        })
    }
    
    /// Execute a SQL statement with Python parameters and return appropriate results
    /// 
    /// This method accepts raw Python objects and converts them internally.
    /// Iterables (lists, tuples, sets, etc.) are automatically expanded for IN clauses,
    /// except strings and bytes which are treated as single values.
    /// ULTRA-FAST VERSION - optimized for maximum performance
    pub fn execute_with_python_params<'p>(&self, py: Python<'p>, query: String, params: &Bound<PyList>) -> PyResult<Bound<'p, PyAny>> {
        // Just call the regular execute method which already handles Python parameters
        self.execute(py, query, Some(params))
    }
    
    /// Check if connected to the database
    pub fn is_connected<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            let is_connected = pool.lock().await.is_some();
            Ok(is_connected)
        })
    }
    
    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let pool_config = self.pool_config.clone();
        
        future_into_py(py, async move {
            let pool_guard = pool.lock().await;
            if let Some(pool_ref) = pool_guard.as_ref() {
                let state = pool_ref.state();
                // Return the values as a tuple that Python can convert to dict
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
            // Use a more robust check-and-create pattern to avoid race conditions
            let mut pool_guard = pool.lock().await;
            if pool_guard.is_none() {
                // Create pool while holding the lock to prevent race conditions
                let new_pool = PyConnection::establish_pool(config, &pool_config).await?;
                *pool_guard = Some(new_pool);
            }
            drop(pool_guard); // Explicitly drop the lock
            Ok(()) // Just return unit, Python wrapper will return self
        })
    }
    
    /// Exit context manager (async version) 
    pub fn __aexit__<'p>(
        &self, 
        py: Python<'p>,
        _exc_type: Option<Bound<PyAny>>, 
        _exc_value: Option<Bound<PyAny>>, 
        _traceback: Option<Bound<PyAny>>
    ) -> PyResult<Bound<'p, PyAny>> {
        // Don't disconnect on exit - let the pool manage connections
        // This allows for connection reuse and prevents premature disconnection
        future_into_py(py, async move {
            Ok(()) // Return unit, don't disconnect
        })
    }
}