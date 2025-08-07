use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyType};
use pyo3::prelude::*;
use tiberius::Row;

/// Ultra-fast direct conversion from Tiberius Row to Python objects
/// Eliminates the intermediate PyValue layer for maximum performance
#[pyclass(name = "FastRow")]
pub struct PyFastRow {
    // Pre-converted values stored directly - no mutex needed since we convert eagerly
    cached_values: std::collections::HashMap<String, PyObject>,
    column_names: Vec<String>,
}

impl Clone for PyFastRow {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            let mut cloned_values = std::collections::HashMap::with_capacity(self.cached_values.len());
            for (key, value) in &self.cached_values {
                cloned_values.insert(key.clone(), value.clone_ref(py));
            }
            PyFastRow {
                cached_values: cloned_values,
                column_names: self.column_names.clone(),
            }
        })
    }
}

impl PyFastRow {
    pub fn from_tiberius_row(row: Row, py: Python) -> PyResult<Self> {
        let column_names: Vec<String> = row.columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();
        
        // Eagerly convert all values - single allocation, no locks
        let mut cached_values = std::collections::HashMap::with_capacity(column_names.len());
        for (index, column_name) in column_names.iter().enumerate() {
            let value = Self::extract_value_direct(&row, index, py)?;
            cached_values.insert(column_name.clone(), value);
        }
        
        Ok(PyFastRow {
            cached_values,
            column_names,
        })
    }

    /// Convert value directly from Tiberius to Python - zero intermediate allocations
    #[inline]
    fn extract_value_direct(row: &Row, index: usize, py: Python) -> PyResult<PyObject> {
        use tiberius::ColumnType;
        
        let col_type = row.columns()[index].column_type();
        
        // Debug logging - temporarily print column type for debugging
        // eprintln!("Column {} type: {:?}", index, col_type);
        
        // Direct conversion - no intermediate PyValue
        match col_type {
            ColumnType::Int4 => {
                match row.try_get::<i32, usize>(index) {
                    Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::NVarchar => {
                match row.try_get::<&str, usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Bit | ColumnType::Bitn => {
                // Debug: Check what we're getting
                match row.try_get::<bool, usize>(index) {
                    Ok(Some(val)) => {
                        let py_bool = val.into_pyobject(py)?;
                        Ok(py_bool.to_owned().into_any().unbind())
                    },
                    Ok(None) => {
                        // This is a SQL NULL value
                        Ok(py.None())
                    },
                    Err(_e) => {
                        // Try as other types that might be returned for BIT
                        if let Ok(Some(val)) = row.try_get::<i32, usize>(index) {
                            let bool_val = val != 0;
                            let py_bool = bool_val.into_pyobject(py)?;
                            Ok(py_bool.to_owned().into_any().unbind())
                        } else if let Ok(Some(val)) = row.try_get::<u8, usize>(index) {
                            let bool_val = val != 0;
                            let py_bool = bool_val.into_pyobject(py)?;
                            Ok(py_bool.to_owned().into_any().unbind())
                        } else {
                            // Fallback to None
                            Ok(py.None())
                        }
                    }
                }
            }
            ColumnType::Int8 => {
                match row.try_get::<i64, usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Float8 => {
                match row.try_get::<f64, usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Float4 => {
                match row.try_get::<f32, usize>(index) {
                    Ok(Some(val)) => Ok((val as f64).into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Money => {
                // SQL Server MONEY type - try multiple conversion approaches
                if let Ok(Some(val)) = row.try_get::<f64, usize>(index) {
                    Ok(val.into_pyobject(py)?.into_any().unbind())
                } else if let Ok(Some(val)) = row.try_get::<i64, usize>(index) {
                    // Money might be returned as scaled integer (cents)
                    let money_val = (val as f64) / 10000.0; // SQL Server MONEY has 4 decimal places
                    Ok(money_val.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            ColumnType::Money4 => {
                // SQL Server SMALLMONEY type - try multiple conversion approaches  
                if let Ok(Some(val)) = row.try_get::<f32, usize>(index) {
                    Ok((val as f64).into_pyobject(py)?.into_any().unbind())
                } else if let Ok(Some(val)) = row.try_get::<i32, usize>(index) {
                    // SmallMoney might be returned as scaled integer (cents)
                    let money_val = (val as f64) / 10000.0; // SQL Server SMALLMONEY has 4 decimal places
                    Ok(money_val.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            // Handle all other SQL Server types with direct conversion
            ColumnType::Int1 => {
                match row.try_get::<u8, usize>(index) {
                    Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Int2 => {
                match row.try_get::<i16, usize>(index) {
                    Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::BigVarChar | ColumnType::NChar | ColumnType::BigChar => {
                match row.try_get::<&str, usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::BigBinary | ColumnType::BigVarBin | ColumnType::Image => {
                match row.try_get::<&[u8], usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Decimaln | ColumnType::Numericn => {
                // Try numeric first, fallback to f64
                if let Ok(Some(numeric)) = row.try_get::<tiberius::numeric::Numeric, usize>(index) {
                    let float_val: f64 = numeric.into();
                    Ok(float_val.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime2 => {
                match row.try_get::<chrono::NaiveDateTime, usize>(index) {
                    Ok(Some(val)) => {
                        let formatted = val.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                        Ok(formatted.into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Daten => {
                match row.try_get::<chrono::NaiveDate, usize>(index) {
                    Ok(Some(val)) => {
                        let formatted = val.format("%Y-%m-%d").to_string();
                        Ok(formatted.into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Timen => {
                match row.try_get::<chrono::NaiveTime, usize>(index) {
                    Ok(Some(val)) => {
                        let formatted = val.format("%H:%M:%S%.f").to_string();
                        Ok(formatted.into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::DatetimeOffsetn => {
                match row.try_get::<chrono::DateTime<chrono::Utc>, usize>(index) {
                    Ok(Some(val)) => {
                        Ok(val.to_rfc3339().into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Guid => {
                match row.try_get::<uuid::Uuid, usize>(index) {
                    Ok(Some(val)) => Ok(val.to_string().into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
            ColumnType::Xml => {
                if let Ok(Some(xml_data)) = row.try_get::<&tiberius::xml::XmlData, usize>(index) {
                    Ok(xml_data.to_string().into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            // Fallback to string for unknown types
            _ => {
                match row.try_get::<&str, usize>(index) {
                    Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(py.None())
                }
            }
        }
    }
}

#[pymethods]
impl PyFastRow {
    /// Lazy column access - only convert when requested
    pub fn __getitem__(&self, py: Python, key: Bound<PyAny>) -> PyResult<PyObject> {
        let column_name = if let Ok(name) = key.extract::<String>() {
            name
        } else if let Ok(index) = key.extract::<usize>() {
            if index < self.column_names.len() {
                self.column_names[index].clone()
            } else {
                return Err(PyValueError::new_err("Column index out of range"));
            }
        } else {
            return Err(PyValueError::new_err("Key must be string or integer"));
        };

        // Get from cache (all values are pre-cached)
        if let Some(cached) = self.cached_values.get(&column_name) {
            Ok(cached.clone_ref(py))
        } else {
            Err(PyValueError::new_err(format!("Column '{}' not found", column_name)))
        }
    }

    /// Get all column names
    pub fn columns(&self) -> Vec<String> {
        self.column_names.clone()
    }

    /// Get number of columns
    pub fn __len__(&self) -> usize {
        self.column_names.len()
    }

    /// Get a specific column value by name
    pub fn get(&self, py: Python, column: &str) -> PyResult<PyObject> {
        self.__getitem__(py, column.into_pyobject(py)?.into_any())
    }

    /// Get a value by column index
    pub fn get_by_index(&self, py: Python, index: usize) -> PyResult<PyObject> {
        self.__getitem__(py, index.into_pyobject(py)?.into_any())
    }

    /// Get all values as a list (in column order)
    pub fn values(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let mut result = Vec::with_capacity(self.column_names.len());
        
        for column_name in &self.column_names {
            if let Some(cached) = self.cached_values.get(column_name) {
                result.push(cached.clone_ref(py));
            } else {
                result.push(py.None());
            }
        }
        
        Ok(result)
    }

    /// Convert to dictionary - batch conversion for efficiency
    pub fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        
        for column_name in &self.column_names {
            if let Some(cached) = self.cached_values.get(column_name) {
                dict.set_item(column_name, cached.clone_ref(py))?;
            } else {
                dict.set_item(column_name, py.None())?;
            }
        }
        
        Ok(dict.into())
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!("FastRow with {} columns", self.column_names.len())
    }

    /// Detailed representation
    pub fn __repr__(&self) -> String {
        format!("FastRow(columns={:?})", self.column_names)
    }
}

/// Optimized execution result that can return either FastRow objects or affected count
#[pyclass(name = "FastExecutionResult")]
pub struct PyFastExecutionResult {
    rows: Option<Vec<PyFastRow>>,
    affected_rows: Option<u64>,
}

#[pymethods]
impl PyFastExecutionResult {
    /// Get the returned rows (if any) - return as Python list that can be indexed
    pub fn rows(&self, py: Python) -> PyResult<PyObject> {
        match &self.rows {
            Some(rows) => {
                let py_list = pyo3::types::PyList::empty(py);
                for row in rows.iter() {
                    // Create a new PyCell for each row to satisfy PyO3's ownership requirements
                    let py_row = Py::new(py, row.clone())?;
                    py_list.append(py_row)?;
                }
                Ok(py_list.into())
            }
            None => Ok(py.None())
        }
    }
    
    /// Get the number of affected rows (if applicable)
    pub fn affected_rows(&self) -> Option<u64> {
        self.affected_rows
    }
    
    /// Check if this result contains rows
    pub fn has_rows(&self) -> bool {
        self.rows.is_some() && !self.rows.as_ref().unwrap().is_empty()
    }
    
    /// Check if this result contains affected row count
    pub fn has_affected_count(&self) -> bool {
        self.affected_rows.is_some()
    }

    /// Get row count (number of returned rows, not affected rows)
    pub fn row_count(&self) -> usize {
        self.rows.as_ref().map_or(0, |rows| rows.len())
    }

    /// Create a result with affected row count (class method for Python)
    #[classmethod]
    pub fn _with_affected_count(_cls: &Bound<PyType>, count: u64) -> Self {
        Self {
            rows: None,
            affected_rows: Some(count),
        }
    }
}

impl PyFastExecutionResult {
    /// Create a result with rows - zero-copy conversion from Tiberius rows
    pub fn with_rows(tiberius_rows: Vec<tiberius::Row>, py: Python) -> PyResult<Self> {
        let mut fast_rows = Vec::with_capacity(tiberius_rows.len());
        
        for row in tiberius_rows.into_iter() {
            fast_rows.push(PyFastRow::from_tiberius_row(row, py)?);
        }
        
        Ok(Self {
            rows: Some(fast_rows),
            affected_rows: None,
        })
    }
    
    /// Create a placeholder result that will have rows added later
    pub fn placeholder_for_rows() -> Self {
        Self {
            rows: None,
            affected_rows: None,
        }
    }
    
    /// Set rows from Tiberius rows - used when we need to convert after async operation
    pub fn set_rows_from_tiberius(&mut self, tiberius_rows: Vec<tiberius::Row>, py: Python) -> PyResult<()> {
        let mut fast_rows = Vec::with_capacity(tiberius_rows.len());
        
        for row in tiberius_rows.into_iter() {
            fast_rows.push(PyFastRow::from_tiberius_row(row, py)?);
        }
        
        self.rows = Some(fast_rows);
        Ok(())
    }
    
    /// Create a result with affected row count
    pub fn with_affected_count(count: u64) -> Self {
        Self {
            rows: None,
            affected_rows: Some(count),
        }
    }
}
