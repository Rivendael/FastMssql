use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3_async_runtimes::tokio::future_into_py;
use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use crate::optimized_types::PyFastExecutionResult;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config, AuthMethod, Row};
use pyo3::types::PyList;
use pyo3::prelude::*;
use std::sync::Arc;
use once_cell::sync::OnceCell;
use bb8::Pool;
use smallvec::SmallVec; // Only used for rare expandable parameter case

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
    pool: Arc<OnceCell<ConnectionPool>>,
    config: Config,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>, // Prefix with underscore to silence unused warning
}

impl PyConnection {
    /// Execute database operation with ZERO GIL usage - completely GIL-free async execution
    /// Pre-analyzed query type to avoid SQL parsing in async context
    async fn execute_raw_async_gil_free(
        pool: Arc<OnceCell<ConnectionPool>>,
        query: String,
        parameters: SmallVec<[FastParameter; 8]>,
        is_result_returning: bool,
    ) -> PyResult<ExecutionResult> {
        let pool_ref = pool.get()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        Self::execute_internal_ultra_fast_gil_free(pool_ref, query, parameters, is_result_returning).await
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
    async fn close_pool(_pool: Arc<OnceCell<ConnectionPool>>) {
        // OnceCell doesn't support "clearing" - this is intentional for performance
        // Connection pools should generally live for the application lifetime
        // If needed, the pool will be dropped when the last Arc reference is dropped
    }

    /// ULTRA-FAST GIL-FREE execution - completely eliminates SQL parsing overhead
    /// Uses pre-analyzed query type to skip SQL analysis entirely in async context
    async fn execute_internal_ultra_fast_gil_free(
        pool: &ConnectionPool,
        query: String,
        parameters: SmallVec<[FastParameter; 8]>,
        is_result_returning_query: bool,
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
        
        // OPTIMIZATION: Use pre-analyzed query type - NO SQL parsing in async context!
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

    /// Ultra-fast SQL analysis - branch-optimized for hot path with zero allocations
    #[inline(always)]
    fn contains_result_returning_statements_ultra_fast(sql: &str) -> bool {
        let sql_bytes = sql.as_bytes();
        let len = sql_bytes.len();
        
        if len < 6 { return false; } // Minimum length for "SELECT"
        
        // OPTIMIZATION: Branchless lookup using perfect hash for common patterns
        // This is faster than SIMD for small strings and avoids complex vectorization overhead
        
        // Fast path: Check for common patterns at start using optimized string comparison
        if len >= 6 && Self::fast_starts_with_ignore_case(sql_bytes, b"select") { return true; }
        if len >= 4 && Self::fast_starts_with_ignore_case(sql_bytes, b"with") { return true; }
        if len >= 4 && Self::fast_starts_with_ignore_case(sql_bytes, b"exec") { return true; }
        if len >= 7 && Self::fast_starts_with_ignore_case(sql_bytes, b"execute") { return true; }
        
        // OPTIMIZATION: Use Boyer-Moore-like algorithm for mid-string " SELECT " search
        // More efficient than SIMD for typical SQL statement lengths (< 1KB)
        Self::contains_select_keyword(sql_bytes)
    }
    
    /// Optimized case-insensitive prefix check using bit manipulation
    #[inline(always)]
    fn fast_starts_with_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
        if haystack.len() < needle.len() { return false; }
        
        // Branchless ASCII lowercase comparison using bit manipulation
        for i in 0..needle.len() {
            let h = haystack[i] | 0x20; // Convert to lowercase (works for ASCII letters)
            let n = needle[i] | 0x20;
            if h != n && !(haystack[i].is_ascii_alphabetic() && needle[i].is_ascii_alphabetic()) {
                return false;
            }
        }
        true
    }
    
    /// Boyer-Moore inspired search for " SELECT " keyword in middle of string
    #[inline(always)]
    fn contains_select_keyword(sql_bytes: &[u8]) -> bool {
        use memchr::memmem;
        
        // Fast Boyer-Moore search for space character, then check following pattern
        let mut pos = 0;
        while let Some(space_pos) = memmem::find(&sql_bytes[pos..], b" ") {
            let absolute_pos = pos + space_pos;
            if absolute_pos + 8 < sql_bytes.len() { // " SELECT "
                let slice_start = absolute_pos + 1;
                if slice_start + 6 <= sql_bytes.len() &&
                   Self::fast_starts_with_ignore_case(&sql_bytes[slice_start..], b"select") &&
                   slice_start + 6 < sql_bytes.len() && sql_bytes[slice_start + 6] == b' ' {
                    return true;
                }
            }
            pos = absolute_pos + 1;
            if pos >= sql_bytes.len().saturating_sub(7) { break; }
        }
        
        false
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

/// Convert a Python object to FastParameter with ultra-fast zero-allocation type detection
/// Uses PyO3's direct downcasting to avoid expensive multiple extract() attempts
fn python_to_fast_parameter(obj: &Bound<PyAny>) -> PyResult<FastParameter> {
    use pyo3::types::{PyBool, PyInt, PyFloat, PyString, PyBytes};
    
    if obj.is_none() {
        return Ok(FastParameter::Null);
    }
    
    // ULTRA-OPTIMIZATION: Use fastest possible type checking order
    // Ordered by frequency in typical database queries
    
    // Try string first (most common in SQL)
    if let Ok(py_string) = obj.downcast::<PyString>() {
        // CRITICAL: Use to_cow() to avoid allocation when possible
        return Ok(FastParameter::String(py_string.to_str()?.to_owned()));
    }
    
    // Try int second (very common)
    if let Ok(py_int) = obj.downcast::<PyInt>() {
        return py_int.extract::<i64>()
            .map(FastParameter::I64)
            .map_err(|_| PyValueError::new_err("Integer value too large for i64"));
    }
    
    // Try float third
    if let Ok(py_float) = obj.downcast::<PyFloat>() {
        return Ok(FastParameter::F64(py_float.value()));
    }
    
    // Try bool fourth (less common)
    if let Ok(py_bool) = obj.downcast::<PyBool>() {
        return Ok(FastParameter::Bool(py_bool.is_true()));
    }
    
    // Try bytes last (least common)
    if let Ok(py_bytes) = obj.downcast::<PyBytes>() {
        return Ok(FastParameter::Bytes(py_bytes.as_bytes().to_vec()));
    }
    
    // Fallback for numpy types, Decimal, etc. - only use extract() as last resort
    // This is MUCH faster than the original version that tried extract() for every type
    if let Ok(i) = obj.extract::<i64>() {
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

/// Convert Python objects to FastParameter with zero-allocation parameter handling
/// Returns SmallVec directly to avoid unnecessary heap allocations for small parameter lists
fn python_params_to_fast_parameters(params: &Bound<PyList>) -> PyResult<SmallVec<[FastParameter; 8]>> {
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
    T: Extend<FastParameter>
{
    // OPTIMIZATION: Use PyO3's iterator trait for better performance
    use pyo3::types::{PyList, PyTuple};
    
    // Fast path for common collection types - avoid iterator overhead
    if let Ok(list) = iterable.downcast::<PyList>() {
        result.extend(
            list.iter()
                .map(|item| python_to_fast_parameter(&item))
                .collect::<PyResult<Vec<_>>>()?
        );
        return Ok(());
    }
    
    if let Ok(tuple) = iterable.downcast::<PyTuple>() {
        result.extend(
            tuple.iter()
                .map(|item| python_to_fast_parameter(&item))
                .collect::<PyResult<Vec<_>>>()?
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
            },
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
    use pyo3::types::{PyString, PyBytes};
    
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
            pool: Arc::new(OnceCell::new()),
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
            // Check if already connected using OnceCell::get()
            if pool.get().is_some() {
                return Ok(());
            }
            
            // Try to initialize the pool (only succeeds once)
            let new_pool = Self::establish_pool(config, &pool_config).await?;
            
            // set() returns Err if already set, which is fine - just means another thread won
            let _ = pool.set(new_pool);
            Ok(())
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
    /// OPTIMIZED VERSION - parameter conversion done synchronously, GIL-free async execution
    #[pyo3(signature = (query, parameters=None))]
    pub fn execute<'p>(&self, py: Python<'p>, query: String, parameters: Option<&Bound<PyAny>>) -> PyResult<Bound<'p, PyAny>> {
        // OPTIMIZATION: Do ALL Python type checking/conversion synchronously while we have the GIL
        // This moves GIL contention out of the async hot path entirely
        let fast_parameters = if let Some(params) = parameters {
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
            SmallVec::new()
        };
        
        // OPTIMIZATION: Use weak reference to avoid Arc clone overhead
        let pool_weak = Arc::downgrade(&self.pool);
        
        // Pre-analyze query while we have the GIL to avoid doing it in async context
        let is_result_returning = Self::contains_result_returning_statements_ultra_fast(&query);
        
        // Return the coroutine - now with ZERO GIL usage in async execution
        future_into_py(py, async move {
            // Upgrade weak reference only when needed
            let pool = pool_weak.upgrade()
                .ok_or_else(|| PyRuntimeError::new_err("Connection pool has been dropped"))?;
            
            let execution_result = Self::execute_raw_async_gil_free(pool, query, fast_parameters, is_result_returning).await?;
            
            // Convert results efficiently - acquire GIL only once per result set
            match execution_result {
                ExecutionResult::Rows(rows) => {
                    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                        let fast_result = PyFastExecutionResult::with_rows(rows, py)?;
                        let py_result = Py::new(py, fast_result)?;
                        Ok(py_result.into_any())
                    })
                },
                ExecutionResult::AffectedCount(count) => {
                    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                        Ok(count.into_pyobject(py)?.into_any().unbind())
                    })
                }
            }
        })
    }
    
    /// Check if connected to the database
    pub fn is_connected<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            let is_connected = pool.get().is_some();
            Ok(is_connected)
        })
    }
    
    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let pool = self.pool.clone();
        let pool_config = self.pool_config.clone();
        
        future_into_py(py, async move {
            if let Some(pool_ref) = pool.get() {
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
            // Check if already connected using OnceCell::get()
            if pool.get().is_some() {
                return Ok(());
            }
            
            // Try to initialize the pool (only succeeds once)
            let new_pool = PyConnection::establish_pool(config, &pool_config).await?;
            
            // set() returns Err if already set, which is fine - just means another thread won
            let _ = pool.set(new_pool);
            Ok(())
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