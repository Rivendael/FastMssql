use crate::connection::PyConnection;
use pyo3::exceptions::PyValueError;
use crate::types::{PyValue};
use pyo3::types::PyList;
use pyo3::prelude::*;

/// A parameterized SQL query
#[pyclass(name = "Query")]
pub struct PyQuery {
    sql: String,
    parameters: Vec<PyValue>,
}

#[pymethods]
impl PyQuery {
    #[new]
    pub fn new(sql: String) -> Self {
        PyQuery {
            sql,
            parameters: Vec::new(),
        }
    }
    
    /// Add a parameter to the query
    pub fn add_parameter(&mut self, value: &PyAny) -> PyResult<()> {
        let py_value = python_to_pyvalue(value)?;
        self.parameters.push(py_value);
        Ok(())
    }
    
    /// Set all parameters at once
    pub fn set_parameters(&mut self, params: &PyList) -> PyResult<()> {
        self.parameters.clear();
        for param in params.iter() {
            self.add_parameter(param)?;
        }
        Ok(())
    }
    
    /// Get the SQL string
    pub fn get_sql(&self) -> String {
        self.sql.clone()
    }
    
    /// Get the parameters
    pub fn get_parameters(&self) -> Vec<PyValue> {
        self.parameters.clone()
    }
    
    /// Execute the query on a connection
    pub fn execute<'p>(&self, py: Python<'p>, connection: &PyConnection) -> PyResult<&'p PyAny> {
        connection.execute_with_params(py, self.sql.clone(), self.parameters.clone())
    }
    
    /// String representation
    pub fn __str__(&self) -> String {
        format!("Query: {} (with {} parameters)", self.sql, self.parameters.len())
    }
    
    /// Representation
    pub fn __repr__(&self) -> String {
        format!("PyQuery(sql='{}', parameters={:?})", self.sql, self.parameters)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_creation() {
        let query = PyQuery::new("SELECT * FROM users".to_string());
        assert_eq!(query.get_sql(), "SELECT * FROM users");
        assert_eq!(query.get_parameters().len(), 0);
    }

    #[test]
    fn test_query_sql_property() {
        let query = PyQuery::new("SELECT * FROM products WHERE price > 100".to_string());
        assert_eq!(query.get_sql(), "SELECT * FROM products WHERE price > 100");
    }

    #[test]
    fn test_query_string_representation() {
        let query = PyQuery::new("SELECT * FROM users".to_string());
        let str_repr = query.__str__();
        assert!(str_repr.contains("SELECT * FROM users"));
        assert!(str_repr.contains("0 parameters"));
    }

    #[test]
    fn test_query_repr() {
        let query = PyQuery::new("SELECT * FROM users".to_string());
        let repr = query.__repr__();
        assert!(repr.contains("PyQuery"));
        assert!(repr.contains("SELECT * FROM users"));
    }
}