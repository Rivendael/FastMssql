use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::PyValueError;
use tiberius::Row;
use tiberius::numeric::Numeric;
use tiberius::xml::XmlData;
use std::collections::HashMap;
use chrono::{DateTime, NaiveDate, NaiveTime, NaiveDateTime, Utc};
use uuid::Uuid;

/// A Python-compatible representation of a database row
#[pyclass(name = "Row")]
#[derive(Clone)]
pub struct PyRow {
    data: HashMap<String, PyValue>,
    columns: Vec<String>,
}

#[pymethods]
impl PyRow {
    /// Get a value by column name
    pub fn get(&self, column: &str) -> PyResult<Option<PyValue>> {
        Ok(self.data.get(column).cloned())
    }
    
    /// Get a value by column index
    pub fn get_by_index(&self, index: usize) -> PyResult<Option<PyValue>> {
        if index < self.columns.len() {
            let column = &self.columns[index];
            Ok(self.data.get(column).cloned())
        } else {
            Ok(None)
        }
    }
    
    /// Get all column names
    pub fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }
    
    /// Get all values as a list
    pub fn values(&self) -> Vec<PyValue> {
        self.columns.iter()
            .map(|col| self.data.get(col).cloned().unwrap_or(PyValue::new_null()))
            .collect()
    }
    
    /// Convert to dictionary
    pub fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (key, value) in &self.data {
            dict.set_item(key, value.to_python(py)?)?;
        }
        Ok(dict.into())
    }
    
    /// Get the number of columns
    pub fn __len__(&self) -> usize {
        self.columns.len()
    }
    
    /// Support indexing by column name or index
    pub fn __getitem__(&self, key: &PyAny) -> PyResult<PyValue> {
        if let Ok(column_name) = key.extract::<String>() {
            self.data.get(&column_name)
                .cloned()
                .ok_or_else(|| PyValueError::new_err(format!("Column '{}' not found", column_name)))
        } else if let Ok(index) = key.extract::<usize>() {
            self.get_by_index(index)?
                .ok_or_else(|| PyValueError::new_err(format!("Index {} out of range", index)))
        } else {
            Err(PyValueError::new_err("Key must be string or integer"))
        }
    }
}

impl PyRow {
    pub fn from_tiberius_row(row: Row) -> PyResult<Self> {
        let mut data = HashMap::new();
        let mut columns = Vec::new();
        
        for (i, column) in row.columns().iter().enumerate() {
            let column_name = column.name().to_string();
            columns.push(column_name.clone());
            
            // Try to extract values based on SQL Server column type
            let value = match column.column_type() {
                tiberius::ColumnType::Bit => {
                    match row.try_get::<bool, usize>(i) {
                        Ok(Some(val)) => PyValue::new_bool(val),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Int1 => {
                    match row.try_get::<u8, usize>(i) {
                        Ok(Some(val)) => PyValue::new_int(val as i64),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Int2 => {
                    match row.try_get::<i16, usize>(i) {
                        Ok(Some(val)) => PyValue::new_int(val as i64),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Int4 => {
                    match row.try_get::<i32, usize>(i) {
                        Ok(Some(val)) => PyValue::new_int(val as i64),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Int8 => {
                    match row.try_get::<i64, usize>(i) {
                        Ok(Some(val)) => PyValue::new_int(val),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Float4 => {
                    match row.try_get::<f32, usize>(i) {
                        Ok(Some(val)) => PyValue::new_float(val as f64),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                tiberius::ColumnType::Float8 => {
                    match row.try_get::<f64, usize>(i) {
                        Ok(Some(val)) => PyValue::new_float(val),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                // String types (NVarchar, BigVarChar, etc.)
                tiberius::ColumnType::NVarchar | 
                tiberius::ColumnType::BigVarChar | 
                tiberius::ColumnType::NChar | 
                tiberius::ColumnType::BigChar => {
                    match row.try_get::<&str, usize>(i) {
                        Ok(Some(val)) => PyValue::new_string(val.to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                // Decimal/Numeric types - SQL Server uses these for exact numeric values
                tiberius::ColumnType::Decimaln | 
                tiberius::ColumnType::Numericn => {
                    // Try to get as Numeric type first
                    match row.try_get::<Numeric, usize>(i) {
                        Ok(Some(numeric)) => {
                            // Convert Numeric to f64
                            let float_val: f64 = numeric.into();
                            PyValue::new_float(float_val)
                        },
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string conversion
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => {
                                    if let Ok(parsed) = val.parse::<f64>() {
                                        PyValue::new_float(parsed)
                                    } else {
                                        PyValue::new_string(val.to_string())
                                    }
                                },
                                Ok(None) => PyValue::new_null(),
                                Err(_) => PyValue::new_null(),
                            }
                        }
                    }
                }
                // Date and Time types
                tiberius::ColumnType::Daten => {
                    match row.try_get::<NaiveDate, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.format("%Y-%m-%d").to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                tiberius::ColumnType::Timen => {
                    match row.try_get::<NaiveTime, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.format("%H:%M:%S%.f").to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                tiberius::ColumnType::Datetime => {
                    match row.try_get::<NaiveDateTime, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                tiberius::ColumnType::Datetimen => {
                    match row.try_get::<NaiveDateTime, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                tiberius::ColumnType::Datetime2 => {
                    match row.try_get::<NaiveDateTime, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                tiberius::ColumnType::DatetimeOffsetn => {
                    match row.try_get::<DateTime<Utc>, usize>(i) {
                        Ok(Some(val)) => PyValue::new_datetime(val.to_rfc3339()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                // Binary types
                tiberius::ColumnType::BigBinary | 
                tiberius::ColumnType::BigVarBin | 
                tiberius::ColumnType::Image => {
                    match row.try_get::<&[u8], usize>(i) {
                        Ok(Some(val)) => PyValue::new_bytes(val.to_vec()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => PyValue::new_null(),
                    }
                }
                // GUID/UniqueIdentifier
                tiberius::ColumnType::Guid => {
                    match row.try_get::<Uuid, usize>(i) {
                        Ok(Some(val)) => PyValue::new_string(val.to_string()),
                        Ok(None) => PyValue::new_null(),
                        Err(_) => {
                            // Fallback to string
                            match row.try_get::<&str, usize>(i) {
                                Ok(Some(val)) => PyValue::new_string(val.to_string()),
                                _ => PyValue::new_null(),
                            }
                        }
                    }
                }
                // XML type
                tiberius::ColumnType::Xml => {
                    // Try XmlData first
                    if let Ok(Some(xml_data)) = row.try_get::<&XmlData, usize>(i) {
                        PyValue::new_string(xml_data.to_string())
                    }
                    // Fallback to string
                    else if let Ok(Some(val)) = row.try_get::<&str, usize>(i) {
                        PyValue::new_string(val.to_string())
                    }
                    else {
                        PyValue::new_null()
                    }
                }
                // For other types, try string first, then fallback to generic approaches
                _ => {
                    // Try string first
                    if let Ok(Some(val)) = row.try_get::<&str, usize>(i) {
                        PyValue::new_string(val.to_string())
                    }
                    // Try i32
                    else if let Ok(Some(val)) = row.try_get::<i32, usize>(i) {
                        PyValue::new_int(val as i64)
                    }
                    // Try i64
                    else if let Ok(Some(val)) = row.try_get::<i64, usize>(i) {
                        PyValue::new_int(val)
                    }
                    // Try f64
                    else if let Ok(Some(val)) = row.try_get::<f64, usize>(i) {
                        PyValue::new_float(val)
                    }
                    // Try bool
                    else if let Ok(Some(val)) = row.try_get::<bool, usize>(i) {
                        PyValue::new_bool(val)
                    }
                    // Try datetime types as fallback
                    else if let Ok(Some(val)) = row.try_get::<NaiveDateTime, usize>(i) {
                        PyValue::new_datetime(val.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    }
                    // If all conversions fail or value is NULL
                    else {
                        PyValue::new_null()
                    }
                }
            };
            
            data.insert(column_name, value);
        }
        
        Ok(PyRow { data, columns })
    }
}

/// A Python-compatible representation of a database value
#[pyclass(name = "Value")]
#[derive(Clone, Debug)]
pub struct PyValue {
    inner: PyValueInner,
}

#[derive(Clone, Debug)]
pub enum PyValueInner {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    DateTime(String), // Store as ISO string for Python compatibility
}

impl PyValue {
    pub fn new_null() -> Self {
        Self { inner: PyValueInner::Null }
    }
    
    pub fn new_bool(value: bool) -> Self {
        Self { inner: PyValueInner::Bool(value) }
    }
    
    pub fn new_int(value: i64) -> Self {
        Self { inner: PyValueInner::Int(value) }
    }
    
    pub fn new_float(value: f64) -> Self {
        Self { inner: PyValueInner::Float(value) }
    }
    
    pub fn new_string(value: String) -> Self {
        Self { inner: PyValueInner::String(value) }
    }
    
    pub fn new_bytes(value: Vec<u8>) -> Self {
        Self { inner: PyValueInner::Bytes(value) }
    }
    
    pub fn new_datetime(value: String) -> Self {
        Self { inner: PyValueInner::DateTime(value) }
    }
}

// Convenience constants
impl PyValue {
    pub const NULL: PyValue = PyValue { inner: PyValueInner::Null };
}

#[pymethods]
impl PyValue {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self.inner, PyValueInner::Null)
    }
    
    /// Convert to Python object
    pub fn to_python(&self, py: Python) -> PyResult<PyObject> {
        match &self.inner {
            PyValueInner::Null => Ok(py.None()),
            PyValueInner::Bool(b) => Ok(b.to_object(py)),
            PyValueInner::Int(i) => Ok(i.to_object(py)),
            PyValueInner::Float(f) => Ok(f.to_object(py)),
            PyValueInner::String(s) => Ok(s.to_object(py)),
            PyValueInner::Bytes(b) => Ok(b.to_object(py)),
            PyValueInner::DateTime(s) => Ok(s.to_object(py)),
        }
    }
    
    /// String representation
    pub fn __str__(&self) -> String {
        match &self.inner {
            PyValueInner::Null => "None".to_string(),
            PyValueInner::Bool(b) => b.to_string(),
            PyValueInner::Int(i) => i.to_string(),
            PyValueInner::Float(f) => f.to_string(),
            PyValueInner::String(s) => s.clone(),
            PyValueInner::Bytes(b) => format!("{:?}", b),
            PyValueInner::DateTime(s) => s.clone(),
        }
    }
    
    /// Representation
    pub fn __repr__(&self) -> String {
        match &self.inner {
            PyValueInner::Null => "PyValue.Null".to_string(),
            PyValueInner::Bool(b) => format!("PyValue.Bool({})", b),
            PyValueInner::Int(i) => format!("PyValue.Int({})", i),
            PyValueInner::Float(f) => format!("PyValue.Float({})", f),
            PyValueInner::String(s) => format!("PyValue.String('{}')", s),
            PyValueInner::Bytes(b) => format!("PyValue.Bytes({:?})", b),
            PyValueInner::DateTime(s) => format!("PyValue.DateTime('{}')", s),
        }
    }
}