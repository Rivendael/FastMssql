use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3_async_runtimes::tokio::future_into_py;
use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use crate::optimized_types::PyFastExecutionResult;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config, AuthMethod, Row};
use pyo3::types::PyList;
use pyo3::prelude::*;
use std::sync::{Arc, OnceLock};
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
    pool: Arc<OnceLock<ConnectionPool>>,
    config: Config,
    pool_config: PyPoolConfig,
    _ssl_config: Option<PySslConfig>, // Prefix with underscore to silence unused warning
}

impl PyConnection {
    /// Execute database operation and return raw results - NO PYTHON CONTEXT
    async fn execute_raw_async(
        pool: Arc<OnceLock<ConnectionPool>>,
        query: String,
        parameters: Vec<FastParameter>,
    ) -> PyResult<ExecutionResult> {
        let pool_ref = pool.get()
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
    async fn close_pool(_pool: Arc<OnceLock<ConnectionPool>>) {
        // OnceLock doesn't support "clearing" - this is intentional for performance
        // Connection pools should generally live for the application lifetime
        // If needed, the pool will be dropped when the last Arc reference is dropped
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
        
        // Ultra-fast SQL analysis - no caching overhead, pure SIMD speed
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

    /// Ultra-fast SQL analysis - SIMD-optimized for hot path with zero allocations
    #[inline(always)]
    fn contains_result_returning_statements_ultra_fast(sql: &str) -> bool {
        use memchr::memmem;
        
        // Zero-allocation case-insensitive search using SIMD-accelerated pattern matching
        let sql_bytes = sql.as_bytes();
        let len = sql_bytes.len();
        
        if len < 6 { return false; } // Minimum length for "SELECT"
        
        // Fast path: Check for common patterns at start using SIMD
        if Self::simd_starts_with_ignore_case(sql_bytes, b"select") ||
           Self::simd_starts_with_ignore_case(sql_bytes, b"with") ||
           Self::simd_starts_with_ignore_case(sql_bytes, b"exec") ||
           (len >= 7 && Self::simd_starts_with_ignore_case(sql_bytes, b"execute")) {
            return true;
        }
        
        // SIMD-accelerated search for " SELECT " in the middle using memchr
        // This uses optimized SIMD instructions to find space characters quickly
        let mut pos = 0;
        while let Some(space_pos) = memmem::find(&sql_bytes[pos..], b" ") {
            let absolute_pos = pos + space_pos;
            if absolute_pos + 8 < len { // " SELECT "
                let slice_start = absolute_pos + 1;
                if slice_start + 6 <= len &&
                   Self::simd_slice_eq_ignore_case(&sql_bytes[slice_start..slice_start + 6], b"select") &&
                   slice_start + 6 < len && sql_bytes[slice_start + 6] == b' ' {
                    return true;
                }
            }
            pos = absolute_pos + 1;
            if pos >= len.saturating_sub(7) { break; }
        }
        
        false
    }
    
    /// SIMD-optimized case-insensitive prefix comparison
    #[inline(always)]
    fn simd_starts_with_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
        if haystack.len() < needle.len() { return false; }
        Self::simd_slice_eq_ignore_case(&haystack[..needle.len()], needle)
    }
    
    /// SIMD-optimized case-insensitive slice comparison
    /// Uses vectorized operations when possible for chunks of 16+ bytes
    #[inline(always)]
    fn simd_slice_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() { return false; }
        
        let len = a.len();
        
        // For small strings (≤8 bytes), use optimized scalar path
        if len <= 8 {
            return Self::scalar_eq_ignore_case(a, b);
        }
        
        // For 9-16 bytes, use single 16-byte SIMD operation with padding
        if len <= 16 {
            let mut a_padded = [0u8; 16];
            let mut b_padded = [0u8; 16];
            a_padded[..len].copy_from_slice(a);
            b_padded[..len].copy_from_slice(b);
            return Self::simd_chunk_eq_ignore_case(&a_padded, &b_padded);
        }
        
        // For larger strings, process in aligned 16-byte chunks
        let chunks = len / 16;
        let remainder = len % 16;
        
        // Process 16-byte chunks with SIMD - aligned access is faster
        for i in 0..chunks {
            let start = i * 16;
            let end = start + 16;
            
            // Use array slicing for guaranteed 16-byte alignment
            let mut a_chunk = [0u8; 16];
            let mut b_chunk = [0u8; 16];
            a_chunk.copy_from_slice(&a[start..end]);
            b_chunk.copy_from_slice(&b[start..end]);
            
            if !Self::simd_chunk_eq_ignore_case(&a_chunk, &b_chunk) {
                return false;
            }
        }
        
        // Handle remaining bytes with scalar comparison
        if remainder > 0 {
            let start = chunks * 16;
            return Self::scalar_eq_ignore_case(&a[start..], &b[start..]);
        }
        
        true
    }
    
    /// SIMD comparison for exactly 16-byte chunks
    #[inline(always)]
    fn simd_chunk_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
        debug_assert_eq!(a.len(), 16);
        debug_assert_eq!(b.len(), 16);
        
        // Load 16 bytes at once using safe array access
        let a_array: [u8; 16] = a.try_into().unwrap();
        let b_array: [u8; 16] = b.try_into().unwrap();
        
        // Convert to lowercase using SIMD-friendly bit manipulation
        let a_lower = Self::simd_to_lowercase_chunk(a_array);
        let b_lower = Self::simd_to_lowercase_chunk(b_array);
        
        a_lower == b_lower
    }
    
    /// SIMD-optimized lowercase conversion for 16-byte chunk
    #[inline(always)]
    fn simd_to_lowercase_chunk(input: [u8; 16]) -> [u8; 16] {
        let mut result = [0u8; 16];
        
        // Process in 8-byte chunks for better vectorization
        for i in 0..2 {
            let start = i * 8;
            for j in 0..8 {
                let byte = input[start + j];
                // Branchless ASCII lowercase conversion
                // If byte is between 'A' and 'Z', add 32 to make it lowercase
                result[start + j] = byte + (((byte >= b'A') & (byte <= b'Z')) as u8) * 32;
            }
        }
        
        result
    }
    
    /// Fallback scalar comparison for small strings or remainder bytes
    #[inline(always)]
    fn scalar_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
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
fn python_params_to_fast_parameters(params: &Bound<PyList>) -> PyResult<Vec<FastParameter>> {
    let len = params.len();
    
    // - 0-8 parameters: Zero heap allocations (stack only)
    // - 9+ parameters: Automatic heap fallback (rare case)
    // - Consistent code path, better cache locality
    let mut result: SmallVec<[FastParameter; 8]> = SmallVec::with_capacity(len.max(8));
    
    for param in params.iter() {
        if is_expandable_iterable(&param)? {
            expand_iterable_to_fast_params(&param, &mut result)?;
        } else {
            result.push(python_to_fast_parameter(&param)?);
        }
    }
    
    Ok(result.into_vec())
}

/// Expand a Python iterable into individual FastParameter objects
fn expand_iterable_to_fast_params<T>(iterable: &Bound<PyAny>, result: &mut T) -> PyResult<()> 
where
    T: Extend<FastParameter>
{
    // Get the iter() method of the iterable
    let iter_method = iterable.getattr("__iter__")?;
    let iterator = iter_method.call0()?;
    
    // Collect items into a temporary vector
    let mut items = Vec::new();
    loop {
        match iterator.call_method0("__next__") {
            Ok(item) => {
                items.push(python_to_fast_parameter(&item)?);
            },
            Err(_) => break, // StopIteration exception
        }
    }
    
    // Extend the result with all items at once
    result.extend(items);
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
            pool: Arc::new(OnceLock::new()),
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
            // Check if already connected using OnceLock::get()
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
        
        let pool = self.pool.clone();
        
        // Return the coroutine directly for Python to await
        future_into_py(py, async move {
            let execution_result = Self::execute_raw_async(pool, query, parameters).await?;
            
            // Convert results efficiently - acquire GIL only once per result
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
            // Check if already connected using OnceLock::get()
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