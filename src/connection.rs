use pyo3::exceptions::{PyRuntimeError, PyValueError};
use crate::types::{PyRow, PyExecutionResult, PyValue};
use pyo3_asyncio::tokio::future_into_py;
use crate::pool_config::PyPoolConfig;
use crate::ssl_config::PySslConfig;
use bb8_tiberius::ConnectionManager;
use tokio::sync::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tiberius::{Config, AuthMethod};
use std::sync::Arc;
use bb8::Pool;

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
    /// Helper function to establish a database connection pool
    /// 
    /// Creates a bb8 connection pool with the provided configuration
    async fn establish_pool(config: Config, pool_config: &PyPoolConfig) -> PyResult<ConnectionPool> {
        let manager = ConnectionManager::new(config);
        
        let mut builder = Pool::builder()
            .max_size(pool_config.max_size);
        
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

    /// Helper function to execute a query and automatically determine return type
    /// 
    /// Executes any SQL statement and returns appropriate results:
    /// - For SELECT queries: Returns rows as PyRow objects
    /// - For INSERT/UPDATE/DELETE/DDL: Returns affected row count
    async fn execute_internal(
        pool: Arc<Mutex<Option<ConnectionPool>>>,
        query: String,
    ) -> PyResult<PyExecutionResult> {
        Self::execute_internal_with_params(pool, query, Vec::new()).await
    }

    /// Helper function to execute a parameterized query and automatically determine return type
    /// 
    /// Executes any SQL statement with parameters and returns appropriate results:
    /// - For SELECT queries: Returns rows as PyRow objects
    /// - For INSERT/UPDATE/DELETE/DDL: Returns affected row count
    async fn execute_internal_with_params(
        pool: Arc<Mutex<Option<ConnectionPool>>>,
        query: String,
        parameters: Vec<PyValue>,
    ) -> PyResult<PyExecutionResult> {
        let pool_guard = pool.lock().await;
        let pool_ref = pool_guard.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        let mut conn = pool_ref.get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e)))?;
        
        // Convert PyValue parameters to Tiberius parameter types
        let mut sql_params = Vec::new();
        for param in &parameters {
            sql_params.push(param.to_sql()
                .map_err(|e| PyRuntimeError::new_err(format!("Parameter conversion failed: {}", e)))?);
        }
        let tiberius_params: Vec<&dyn tiberius::ToSql> = sql_params.iter()
            .map(|p| p.as_ref() as &dyn tiberius::ToSql)
            .collect();
        
        // Improved SQL analysis to detect if the batch might return results
        // This handles multi-statement batches, comments, and complex scenarios
        let trimmed_query = query.trim();
        let is_result_returning_query = Self::contains_result_returning_statements(&trimmed_query);
        
        if is_result_returning_query {
            // Use query() for statements that return results
            let stream = conn.query(&query, &tiberius_params)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
            
            let rows = stream.into_first_result()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))?;
            
            let py_rows = Self::convert_rows_to_py(rows)?;
            Ok(PyExecutionResult::with_rows(py_rows))
        } else {
            // Use execute() for INSERT/UPDATE/DELETE/DDL to get affected row count
            let result = conn.execute(&query, &tiberius_params)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
            
            let total_affected: u64 = result.rows_affected().iter().sum();
            Ok(PyExecutionResult::with_affected_count(total_affected))
        }
    }

    /// Helper function to convert Tiberius rows to PyRow objects
    /// 
    /// Converts the raw database rows to Python-compatible objects
    fn convert_rows_to_py(rows: Vec<tiberius::Row>) -> PyResult<Vec<PyRow>> {
        let mut py_rows = Vec::with_capacity(rows.len());
        for row in rows {
            py_rows.push(PyRow::from_tiberius_row(row)?);
        }
        Ok(py_rows)
    }
    
    /// Determine if a SQL string contains result-returning statements
    /// 
    /// This function analyzes SQL text to detect if it contains statements that
    /// would return a result set (SELECT, WITH, EXEC procedures that return results, etc.)
    fn contains_result_returning_statements(sql: &str) -> bool {
        // Remove SQL comments and normalize whitespace
        let normalized_sql = Self::remove_sql_comments(sql);
        let lowercased = normalized_sql.to_lowercase();
        
        // Split by semicolons to handle multi-statement batches
        let statements: Vec<&str> = lowercased.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        
        // Check if any statement returns results
        for statement in statements {
            let trimmed = statement.trim();
            
            // Direct SELECT or WITH statements
            if trimmed.starts_with("select") || trimmed.starts_with("with") {
                return true;
            }
            
            // EXEC calls that might return results (heuristic approach)
            if trimmed.starts_with("exec") || trimmed.starts_with("execute") {
                return true;
            }
            
            // Handle complex multi-line statements that might contain SELECT
            // Look for SELECT keyword anywhere in the statement (could be subquery, CTE, etc.)
            if trimmed.contains("select") {
                return true;
            }
        }
        
        false
    }
    
    /// Remove SQL comments from a string
    /// 
    /// Removes both single-line (--) and multi-line (/* */) comments
    fn remove_sql_comments(sql: &str) -> String {
        let mut result = String::new();
        let mut chars = sql.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '-' if chars.peek() == Some(&'-') => {
                    // Single-line comment, skip until newline
                    chars.next(); // consume second '-'
                    while let Some(c) = chars.next() {
                        if c == '\n' || c == '\r' {
                            result.push(c);
                            break;
                        }
                    }
                },
                '/' if chars.peek() == Some(&'*') => {
                    // Multi-line comment, skip until */
                    chars.next(); // consume '*'
                    let mut prev_char = ' ';
                    while let Some(c) = chars.next() {
                        if prev_char == '*' && c == '/' {
                            break;
                        }
                        prev_char = c;
                    }
                    result.push(' '); // Replace comment with space
                },
                _ => result.push(ch),
            }
        }
        
        result
    }
}

/// Convert a Python object to PyValue
fn python_to_pyvalue(obj: &PyAny) -> PyResult<PyValue> {
    if obj.is_none() {
        Ok(PyValue::new_null())
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(PyValue::new_bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(PyValue::new_int(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(PyValue::new_float(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(PyValue::new_string(s))
    } else if let Ok(b) = obj.extract::<Vec<u8>>() {
        Ok(PyValue::new_bytes(b))
    } else {
        Err(PyValueError::new_err(format!("Unsupported parameter type: {}", obj.get_type().name()?)))
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
                config.authentication(AuthMethod::windows("", ""));
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
    pub fn connect<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let pool_config = self.pool_config.clone();
        
        future_into_py(py, async move {
            let new_pool = Self::establish_pool(config, &pool_config).await?;
            *pool.lock().await = Some(new_pool);
            Ok(())
        })
    }
    
    /// Disconnect from the database
    pub fn disconnect<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::close_pool(pool).await;
            Ok(())
        })
    }
    
    /// Execute a SQL statement and return appropriate results
    /// 
    /// For SELECT queries: Returns rows as PyRow objects
    /// For INSERT/UPDATE/DELETE/DDL: Returns affected row count
    /// The result type can be checked using has_rows() and has_affected_count() methods
    pub fn execute<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::execute_internal(pool, query).await
        })
    }
    
    /// Execute a query and return only the rows (backward compatibility)
    /// 
    /// This method is kept for backward compatibility. It executes the query
    /// and returns only the rows if it's a SELECT query, or an empty list otherwise.
    pub fn execute_query<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            let result = Self::execute_internal(pool, query).await?;
            match result.rows() {
                Some(rows) => Ok(rows),
                None => Ok(Vec::<PyRow>::new()), // Return empty vec if no rows
            }
        })
    }

    /// Execute a SQL statement with parameters and return appropriate results
    /// 
    /// For SELECT queries: Returns rows as PyRow objects
    /// For INSERT/UPDATE/DELETE/DDL: Returns affected row count
    /// The result type can be checked using has_rows() and has_affected_count() methods
    pub fn execute_with_params<'p>(&self, py: Python<'p>, query: String, parameters: Vec<PyValue>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::execute_internal_with_params(pool, query, parameters).await
        })
    }

    /// Execute a SQL statement with Python parameters and return appropriate results
    /// 
    /// This method accepts raw Python objects and converts them internally
    pub fn execute_with_python_params<'p>(&self, py: Python<'p>, query: String, params: &PyList) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        // Convert Python objects to PyValue objects
        let mut parameters = Vec::new();
        for param in params.iter() {
            let py_value = python_to_pyvalue(param)?;
            parameters.push(py_value);
        }
        
        future_into_py(py, async move {
            Self::execute_internal_with_params(pool, query, parameters).await
        })
    }
    
    /// Execute a query with parameters and return only the rows (backward compatibility)
    /// 
    /// This method executes the query with parameters and returns only the rows 
    /// if it's a SELECT query, or an empty list otherwise.
    pub fn execute_query_with_params<'p>(&self, py: Python<'p>, query: String, parameters: Vec<PyValue>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            let result = Self::execute_internal_with_params(pool, query, parameters).await?;
            match result.rows() {
                Some(rows) => Ok(rows),
                None => Ok(Vec::<PyRow>::new()), // Return empty vec if no rows
            }
        })
    }
    
    /// Check if connected to the database
    pub fn is_connected<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Ok(pool.lock().await.is_some())
        })
    }
    
    /// Get connection pool statistics
    pub fn pool_stats<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        let pool_config = self.pool_config.clone();
        
        future_into_py(py, async move {
            let pool_guard = pool.lock().await;
            if let Some(pool_ref) = pool_guard.as_ref() {
                let state = pool_ref.state();
                Python::with_gil(|py| {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("connections", state.connections)?;
                    dict.set_item("idle_connections", state.idle_connections)?;
                    dict.set_item("max_size", pool_config.max_size)?;
                    dict.set_item("min_idle", pool_config.min_idle)?;
                    dict.set_item("active_connections", state.connections - state.idle_connections)?;
                    Ok(dict.to_object(py))
                })
            } else {
                Python::with_gil(|py| {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("connected", false)?;
                    Ok(dict.to_object(py))
                })
            }
        })
    }
    
    /// Enter context manager (async version)
    pub fn __aenter__<'p>(slf: &'p PyCell<Self>, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = slf.borrow().pool.clone();
        let config = slf.borrow().config.clone();
        let pool_config = slf.borrow().pool_config.clone();
        let self_obj: PyObject = slf.into();
        
        future_into_py(py, async move {
            let new_pool = PyConnection::establish_pool(config, &pool_config).await?;
            *pool.lock().await = Some(new_pool);
            Ok(self_obj)
        })
    }
    
    /// Exit context manager (async version)
    pub fn __aexit__<'p>(
        &self, 
        py: Python<'p>,
        _exc_type: Option<&PyAny>, 
        _exc_value: Option<&PyAny>, 
        _traceback: Option<&PyAny>
    ) -> PyResult<&'p PyAny> {
        self.disconnect(py)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_connection_creation() {
        let conn_string = env::var("MSSQL_CONNECTION_STRING").unwrap_or_else(|_| {
            "Server=localhost;Database=test;Integrated Security=true".to_string()
        });
        
        let connection = PyConnection::new(Some(conn_string), None, None, None, None, None, None, None).expect("Failed to create connection");
        // Connection object created successfully
        // Actual connection testing would require async runtime and real database
    }
    
    #[test]
    fn test_connection_with_individual_params() {
        let connection = PyConnection::new(
            None, // no connection string
            None, // default pool config
            None, // no SSL config
            Some("localhost".to_string()), // server
            Some("test".to_string()), // database
            None, // no username (will use Windows auth)
            None, // no password
            Some(true) // trusted connection
        ).expect("Failed to create connection with individual params");
        
        // Connection object created successfully
    }
    
    #[test]
    fn test_connection_with_sql_auth() {
        let connection = PyConnection::new(
            None, // no connection string
            None, // default pool config
            None, // no SSL config
            Some("localhost".to_string()), // server
            Some("test".to_string()), // database
            Some("testuser".to_string()), // username
            Some("testpass".to_string()), // password
            Some(false) // not trusted connection
        ).expect("Failed to create connection with SQL auth");
        
        // Connection object created successfully
    }
    
    #[test]
    fn test_connection_requires_server_or_connection_string() {
        let result = PyConnection::new(
            None, // no connection string
            None, // default pool config
            None, // no SSL config
            None, // no server
            None, // no database
            None, // no username
            None, // no password
            None  // no trusted connection
        );
        
        // Should fail because neither connection_string nor server was provided
        assert!(result.is_err());
    }

    #[test]
    fn test_connection_with_ssl_config() {
        let ssl_config = crate::ssl_config::PySslConfig::development();
        let connection = PyConnection::new(
            None, // no connection string
            None, // default pool config
            Some(ssl_config), // SSL config
            Some("localhost".to_string()), // server
            Some("test".to_string()), // database
            None, // no username (will use Windows auth)
            None, // no password
            Some(true) // trusted connection
        ).expect("Failed to create connection with SSL config");
        
        // Connection object created successfully
        assert!(connection.ssl_config.is_some());
    }
}