use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3_asyncio::tokio::future_into_py;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::types::PyRow;

type TiberiusClient = Client<Compat<TcpStream>>;

/// A connection to a Microsoft SQL Server database
#[pyclass(name = "Connection")]
pub struct PyConnection {
    client: Arc<Mutex<Option<TiberiusClient>>>,
    config: Config,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl PyConnection {
    /// Helper function to establish a database connection
    /// 
    /// This function handles the complete connection process including:
    /// - Establishing TCP connection
    /// - Setting TCP_NODELAY for better performance
    /// - Authenticating with the database
    /// - Storing the client instance
    async fn establish_connection(
        client: Arc<Mutex<Option<TiberiusClient>>>,
        config: Config,
    ) -> PyResult<()> {
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;
        
        tcp.set_nodelay(true)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to set nodelay: {}", e)))?;
        
        let client_instance = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to authenticate: {}", e)))?;
        
        *client.lock().await = Some(client_instance);
        Ok(())
    }

    /// Helper function to close a database connection
    /// 
    /// Safely closes the database connection if one exists
    async fn close_connection(client: Arc<Mutex<Option<TiberiusClient>>>) {
        let mut client_guard = client.lock().await;
        if let Some(client_instance) = client_guard.take() {
            let _ = client_instance.close().await;
        }
    }

    /// Helper function to execute a query and return results
    /// 
    /// Executes a SELECT query and returns the results as PyRow objects
    async fn execute_query_internal(
        client: Arc<Mutex<Option<TiberiusClient>>>,
        query: String,
    ) -> PyResult<Vec<PyRow>> {
        let mut client_guard = client.lock().await;
        let client_instance = client_guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        let stream = client_instance.query(&query, &[])
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
        client: Arc<Mutex<Option<TiberiusClient>>>,
        query: String,
    ) -> PyResult<u64> {
        let mut client_guard = client.lock().await;
        let client_instance = client_guard.as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Not connected to database"))?;
        
        let result = client_instance.execute(&query, &[])
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
    pub fn new(connection_string: String) -> PyResult<Self> {
        let config = Config::from_ado_string(&connection_string)
            .map_err(|e| PyValueError::new_err(format!("Invalid connection string: {}", e)))?;
        
        let runtime = Arc::new(
            tokio::runtime::Runtime::new()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?
        );
        
        Ok(PyConnection {
            client: Arc::new(Mutex::new(None)),
            config,
            runtime,
        })
    }
    /// Connect to the database (async version)
    pub fn connect_async<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let client = self.client.clone();
        let config = self.config.clone();
        
        future_into_py(py, async move {
            Self::establish_connection(client, config).await
        })
    }
    
    /// Connect to the database
    pub fn connect(&self) -> PyResult<()> {
        let client = self.client.clone();
        let config = self.config.clone();
        
        self.runtime.block_on(async move {
            Self::establish_connection(client, config).await
        })
    }
    
    /// Disconnect from the database (async version)
    pub fn disconnect_async<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let client = self.client.clone();
        
        future_into_py(py, async move {
            Self::close_connection(client).await;
            Ok(())
        })
    }
    
    /// Disconnect from the database
    pub fn disconnect(&self) -> PyResult<()> {
        let client = self.client.clone();
        
        self.runtime.block_on(async move {
            Self::close_connection(client).await;
        });
        
        Ok(())
    }
    
    /// Execute a query and return the results (async version)
    pub fn execute_async<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let client = self.client.clone();
        
        future_into_py(py, async move {
            Self::execute_query_internal(client, query).await
        })
    }
    
    /// Execute a query and return the results
    pub fn execute(&self, query: String) -> PyResult<Vec<PyRow>> {
        let client = self.client.clone();
        
        self.runtime.block_on(async move {
            Self::execute_query_internal(client, query).await
        })
    }
    
    /// Execute a query without returning results (async version)
    pub fn execute_non_query_async<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let client = self.client.clone();
        
        future_into_py(py, async move {
            Self::execute_non_query_internal(client, query).await
        })
    }
    
    /// Execute a query without returning results (for INSERT, UPDATE, DELETE)
    pub fn execute_non_query(&self, query: String) -> PyResult<u64> {
        let client = self.client.clone();
        
        self.runtime.block_on(async move {
            Self::execute_non_query_internal(client, query).await
        })
    }
    
    /// Check if connected to the database
    pub fn is_connected(&self) -> bool {
        self.runtime.block_on(async {
            self.client.lock().await.is_some()
        })
    }
    
    /// Enter context manager
    pub fn __enter__(slf: PyRef<Self>) -> PyResult<PyRef<Self>> {
        slf.connect()?;
        Ok(slf)
    }
    
    /// Exit context manager
    pub fn __exit__(&self, _exc_type: Option<&PyAny>, _exc_value: Option<&PyAny>, _traceback: Option<&PyAny>) -> PyResult<()> {
        self.disconnect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_connection_establishment() {
        let conn_string = env::var("MSSQL_CONNECTION_STRING").unwrap_or_else(|_| {
            "Server=localhost;Database=test;Integrated Security=true".to_string()
        });
        
        let connection = PyConnection::new(conn_string).expect("Failed to create connection");
        // Note: This test requires a real database connection
        // For unit testing, we might want to mock the database connection
        // For now, we'll just test that the connection object is created successfully
        assert!(!connection.is_connected()); // Should not be connected initially
    }

    #[test] 
    fn test_connection_lifecycle() {
        let conn_string = env::var("MSSQL_CONNECTION_STRING").unwrap_or_else(|_| {
            "Server=localhost;Database=test;Integrated Security=true".to_string()
        });
        
        let connection = PyConnection::new(conn_string).expect("Failed to create connection");
        assert!(!connection.is_connected()); // Initially not connected
        
        // Note: Actual connection test would require a real database
        // For integration tests, set MSSQL_CONNECTION_STRING environment variable
        
        // Test disconnect works even when not connected
        assert!(connection.disconnect().is_ok());
        assert!(!connection.is_connected());
    }
}