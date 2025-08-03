use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3_asyncio::tokio::future_into_py;
use tiberius::Config;
use std::sync::Arc;
use tokio::sync::Mutex;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use crate::types::PyRow;
use crate::pool_config::PyPoolConfig;

type ConnectionPool = Pool<ConnectionManager>;

/// A connection pool to a Microsoft SQL Server database
#[pyclass(name = "Connection")]
pub struct PyConnection {
    pool: Arc<Mutex<Option<ConnectionPool>>>,
    config: Config,
    pool_config: PyPoolConfig,
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

    /// Helper function to execute a query and return results
    /// 
    /// Executes a SELECT query and returns the results as PyRow objects
    async fn execute_query_internal(
        pool: Arc<Mutex<Option<ConnectionPool>>>,
        query: String,
    ) -> PyResult<Vec<PyRow>> {
        let pool_guard = pool.lock().await;
        let pool_ref = pool_guard.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        let mut conn = pool_ref.get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e)))?;
        
        let stream = conn.query(&query, &[])
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
        
        let rows = stream.into_first_result()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get results: {}", e)))?;
        
        Self::convert_rows_to_py(rows)
    }

    /// Helper function to execute a non-query command
    /// 
    /// Executes INSERT, UPDATE, DELETE, or other non-query commands
    /// Returns the number of affected rows
    async fn execute_non_query_internal(
        pool: Arc<Mutex<Option<ConnectionPool>>>,
        query: String,
    ) -> PyResult<u64> {
        let pool_guard = pool.lock().await;
        let pool_ref = pool_guard.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        let mut conn = pool_ref.get()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get connection from pool: {}", e)))?;
        
        let result = conn.execute(&query, &[])
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
        
        // Sum all affected rows from all statements in the batch
        let total_affected: u64 = result.rows_affected().iter().sum();
        Ok(total_affected)
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
}

#[pymethods]
impl PyConnection {
    #[new]
    #[pyo3(signature = (connection_string, pool_config = None))]
    pub fn new(connection_string: String, pool_config: Option<PyPoolConfig>) -> PyResult<Self> {
        let config = Config::from_ado_string(&connection_string)
            .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?;
        
        let pool_config = pool_config.unwrap_or_else(PyPoolConfig::default);
        
        Ok(PyConnection {
            pool: Arc::new(Mutex::new(None)),
            config,
            pool_config,
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
    
    /// Execute a query and return the results
    pub fn execute<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::execute_query_internal(pool, query).await
        })
    }
    
    /// Execute a query without returning results (for INSERT, UPDATE, DELETE)
    pub fn execute_non_query<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        
        future_into_py(py, async move {
            Self::execute_non_query_internal(pool, query).await
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
        
        let connection = PyConnection::new(conn_string, None).expect("Failed to create connection");
        // Connection object created successfully
        // Actual connection testing would require async runtime and real database
    }
}