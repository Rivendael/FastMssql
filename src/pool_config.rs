use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Configuration for the bb8 connection pool
#[pyclass(name = "PoolConfig")]
#[derive(Clone)]
pub struct PyPoolConfig {
    pub max_size: u32,
    pub min_idle: Option<u32>,
    pub max_lifetime: Option<std::time::Duration>,
    pub idle_timeout: Option<std::time::Duration>,
    pub connection_timeout: Option<std::time::Duration>,
    pub test_on_check_out: Option<bool>,
    pub retry_connection: Option<bool>,
}

#[pymethods]
impl PyPoolConfig {
    #[new]
    #[pyo3(signature = (max_size = 20, min_idle = Some(2), max_lifetime_secs = None, idle_timeout_secs = None, connection_timeout_secs = Some(30), test_on_check_out = None, retry_connection = None))]
    pub fn new(
        max_size: u32,
        min_idle: Option<u32>,
        max_lifetime_secs: Option<u64>,
        idle_timeout_secs: Option<u64>,
        connection_timeout_secs: Option<u64>,
        test_on_check_out: Option<bool>,
        retry_connection: Option<bool>,
    ) -> PyResult<Self> {
        if max_size == 0 {
            return Err(PyValueError::new_err("max_size must be greater than 0"));
        }

        if let Some(min) = min_idle {
            if min > max_size {
                return Err(PyValueError::new_err(
                    "min_idle cannot be greater than max_size",
                ));
            }
        }

        Ok(PyPoolConfig {
            max_size,
            min_idle,
            max_lifetime: max_lifetime_secs.map(std::time::Duration::from_secs),
            idle_timeout: idle_timeout_secs.map(std::time::Duration::from_secs),
            connection_timeout: connection_timeout_secs.map(std::time::Duration::from_secs),
            test_on_check_out,
            retry_connection,
        })
    }

    /// Get the maximum number of connections in the pool
    #[getter]
    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    /// Set the maximum number of connections in the pool
    #[setter]
    pub fn set_max_size(&mut self, value: u32) -> PyResult<()> {
        if value == 0 {
            return Err(PyValueError::new_err("max_size must be greater than 0"));
        }
        if let Some(min) = self.min_idle {
            if min > value {
                return Err(PyValueError::new_err(
                    "max_size cannot be less than min_idle",
                ));
            }
        }
        self.max_size = value;
        Ok(())
    }

    /// Get the minimum number of idle connections
    #[getter]
    pub fn min_idle(&self) -> Option<u32> {
        self.min_idle
    }

    /// Set the minimum number of idle connections
    #[setter]
    pub fn set_min_idle(&mut self, value: Option<u32>) -> PyResult<()> {
        if let Some(min) = value {
            if min > self.max_size {
                return Err(PyValueError::new_err(
                    "min_idle cannot be greater than max_size",
                ));
            }
        }
        self.min_idle = value;
        Ok(())
    }

    /// Get the maximum lifetime of connections in seconds
    #[getter]
    pub fn max_lifetime_secs(&self) -> Option<u64> {
        self.max_lifetime.map(|d| d.as_secs())
    }

    /// Set the maximum lifetime of connections in seconds
    #[setter]
    pub fn set_max_lifetime_secs(&mut self, value: Option<u64>) {
        self.max_lifetime = value.map(std::time::Duration::from_secs);
    }

    /// Get the idle timeout in seconds
    #[getter]
    pub fn idle_timeout_secs(&self) -> Option<u64> {
        self.idle_timeout.map(|d| d.as_secs())
    }

    /// Set the idle timeout in seconds
    #[setter]
    pub fn set_idle_timeout_secs(&mut self, value: Option<u64>) {
        self.idle_timeout = value.map(std::time::Duration::from_secs);
    }

    /// Get the connection timeout in seconds
    #[getter]
    pub fn connection_timeout_secs(&self) -> Option<u64> {
        self.connection_timeout.map(|d| d.as_secs())
    }

    /// Set the connection timeout in seconds
    #[setter]
    pub fn set_connection_timeout_secs(&mut self, value: Option<u64>) {
        self.connection_timeout = value.map(std::time::Duration::from_secs);
    }

    /// Get whether to test connections on check out
    #[getter]
    pub fn test_on_check_out(&self) -> Option<bool> {
        self.test_on_check_out
    }

    /// Get whether to retry failed connections
    #[getter]
    pub fn retry_connection(&self) -> Option<bool> {
        self.retry_connection
    }

    /// Create a default configuration for high-throughput scenarios
    /// Optimized for 15-25 concurrent workers without pool contention
    #[staticmethod]
    pub fn high_throughput() -> Self {
        PyPoolConfig {
            max_size: 25,      // Reduced from 50 - sweet spot for most workloads
            min_idle: Some(8), // Reduced from 15 - maintains ready connections
            max_lifetime: Some(std::time::Duration::from_secs(1800)),
            idle_timeout: Some(std::time::Duration::from_secs(600)),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    /// Create a default configuration for single-connection scenarios
    #[staticmethod]
    pub fn one() -> Self {
        PyPoolConfig {
            max_size: 1,
            min_idle: Some(1),
            max_lifetime: Some(std::time::Duration::from_secs(1800)),
            idle_timeout: Some(std::time::Duration::from_secs(300)),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    /// Create a default configuration for low-resource scenarios
    #[staticmethod]
    pub fn low_resource() -> Self {
        PyPoolConfig {
            max_size: 3,
            min_idle: Some(1),
            max_lifetime: Some(std::time::Duration::from_secs(900)),
            idle_timeout: Some(std::time::Duration::from_secs(300)),
            connection_timeout: Some(std::time::Duration::from_secs(15)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    /// Create a default configuration for development scenarios
    #[staticmethod]
    pub fn development() -> Self {
        PyPoolConfig {
            max_size: 5,
            min_idle: Some(1),
            max_lifetime: Some(std::time::Duration::from_secs(600)),
            idle_timeout: Some(std::time::Duration::from_secs(180)),
            connection_timeout: Some(std::time::Duration::from_secs(10)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    /// Create a configuration optimized for maximum performance
    /// Balanced for high concurrency without connection pool contention
    #[staticmethod]
    pub fn performance() -> Self {
        PyPoolConfig {
            max_size: 30,       // Reduced from 100 - prevents lock contention and thundering herd
            min_idle: Some(10), // Reduced from 30 - matches typical concurrent load
            max_lifetime: Some(std::time::Duration::from_secs(7200)),
            idle_timeout: Some(std::time::Duration::from_secs(1800)),
            connection_timeout: Some(std::time::Duration::from_secs(10)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    /// Create an adaptive pool configuration based on expected concurrency
    /// Formula: max_size = ceil(concurrent_workers * 1.2) + 5
    #[staticmethod]
    pub fn adaptive(concurrent_workers: u32) -> Self {
        let max_size = ((concurrent_workers as f64 * 1.2).ceil() as u32 + 5).max(5);
        let min_idle = (max_size / 3).max(2);

        PyPoolConfig {
            max_size,
            min_idle: Some(min_idle),
            max_lifetime: Some(std::time::Duration::from_secs(1800)),
            idle_timeout: Some(std::time::Duration::from_secs(600)),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PoolConfig(max_size={}, min_idle={:?}, max_lifetime_secs={:?}, idle_timeout_secs={:?}, connection_timeout_secs={:?}, test_on_check_out={:?}, retry_connection={:?})",
            self.max_size,
            self.min_idle,
            self.max_lifetime_secs(),
            self.idle_timeout_secs(),
            self.connection_timeout_secs(),
            self.test_on_check_out,
            self.retry_connection
        )
    }
}

impl Default for PyPoolConfig {
    fn default() -> Self {
        PyPoolConfig {
            max_size: 15,      // Balanced default for typical workloads (was 10)
            min_idle: Some(3), // Slightly higher min_idle to keep connections ready
            max_lifetime: Some(std::time::Duration::from_secs(1800)),
            idle_timeout: Some(std::time::Duration::from_secs(300)),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            test_on_check_out: None,
            retry_connection: None,
        }
    }
}
