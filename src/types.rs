use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyType};
use pyo3::prelude::*;
use tiberius::Row;
use ahash::AHashMap as HashMap;
use std::sync::Arc;
use chrono::{Datelike, Timelike};
/// Holds shared column information for a result set to reduce memory usage.
/// This is shared across all `PyFastRow` instances in a result set.
#[derive(Debug)]
pub struct ColumnInfo {
    /// Ordered list of column names
    pub names: Vec<String>,
    /// Map from column name to its index for fast lookups
    pub map: HashMap<String, usize>,
}

/// Memory-optimized to share column metadata across all rows in a result set.
#[pyclass(name = "FastRow")]
pub struct PyFastRow {
    // Row values stored in column order for cache-friendly access
    values: Vec<Py<PyAny>>,
    // Shared pointer to column metadata for the entire result set
    column_info: Arc<ColumnInfo>,
}

impl PyFastRow {
    /// Create a new PyFastRow from a Tiberius row and shared column info
    pub fn from_tiberius_row(row: Row, py: Python, column_info: Arc<ColumnInfo>) -> PyResult<Self> {
        // Eagerly convert all values in column order
        let mut values = Vec::with_capacity(column_info.names.len());
        for i in 0..column_info.names.len() {
            let value = Self::extract_value_direct(&row, i, py)?;
            values.push(value);
        }
        
        Ok(PyFastRow {
            values,
            column_info,
        })
    }

    /// Convert value directly from Tiberius to Python
    #[inline]
    fn extract_value_direct(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
        use tiberius::ColumnType;
        
        let col_type = row.columns()[index].column_type();
        
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
                match row.try_get::<bool, usize>(index) {
                    Ok(Some(val)) => {
                        let int_val = if val { 1i32 } else { 0i32 };
                        Ok(int_val.into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
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
                if let Ok(Some(val)) = row.try_get::<f64, usize>(index) {
                    Ok(val.into_pyobject(py)?.into_any().unbind())
                } else if let Ok(Some(val)) = row.try_get::<i64, usize>(index) {
                    let money_val = (val as f64) / 10000.0; // SQL Server MONEY has 4 decimal places
                    Ok(money_val.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
            ColumnType::Money4 => {
                if let Ok(Some(val)) = row.try_get::<f32, usize>(index) {
                    Ok((val as f64).into_pyobject(py)?.into_any().unbind())
                } else if let Ok(Some(val)) = row.try_get::<i32, usize>(index) {
                    let money_val = (val as f64) / 10000.0; // SQL Server SMALLMONEY has 4 decimal places
                    Ok(money_val.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(py.None())
                }
            }
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
                        // Create Python datetime directly without string intermediate
                        let dt = pyo3::types::PyDateTime::new(
                            py,
                            val.year(),
                            val.month() as u8,
                            val.day() as u8,
                            val.hour() as u8,
                            val.minute() as u8,
                            val.second() as u8,
                            val.nanosecond() / 1000,  // Convert nanoseconds to microseconds
                            None,
                        )?;
                        Ok(dt.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Daten => {
                match row.try_get::<chrono::NaiveDate, usize>(index) {
                    Ok(Some(val)) => {
                        // Create Python date directly without string intermediate
                        let date = pyo3::types::PyDate::new(
                            py,
                            val.year(),
                            val.month() as u8,
                            val.day() as u8,
                        )?;
                        Ok(date.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Timen => {
                match row.try_get::<chrono::NaiveTime, usize>(index) {
                    Ok(Some(val)) => {
                        // Create Python time directly without string intermediate
                        let time = pyo3::types::PyTime::new(
                            py,
                            val.hour() as u8,
                            val.minute() as u8,
                            val.second() as u8,
                            val.nanosecond() / 1000,  // Convert nanoseconds to microseconds
                            None,
                        )?;
                        Ok(time.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::DatetimeOffsetn => {
                match row.try_get::<chrono::DateTime<chrono::Utc>, usize>(index) {
                    Ok(Some(val)) => {
                        // Create Python datetime directly without string intermediate
                        let dt = pyo3::types::PyDateTime::new(
                            py,
                            val.year(),
                            val.month() as u8,
                            val.day() as u8,
                            val.hour() as u8,
                            val.minute() as u8,
                            val.second() as u8,
                            val.nanosecond() / 1000,  // Convert nanoseconds to microseconds
                            None, // For now, skip timezone to avoid conversion overhead
                        )?;
                        Ok(dt.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Guid => {
                match row.try_get::<uuid::Uuid, usize>(index) {
                    Ok(Some(val)) => {
                        // Use most efficient UUID string conversion
                        let uuid_str = format!("{}", val);
                        Ok(uuid_str.into_pyobject(py)?.into_any().unbind())
                    },
                    _ => Ok(py.None())
                }
            }
            ColumnType::Xml => {
                if let Ok(Some(xml_data)) = row.try_get::<&tiberius::xml::XmlData, usize>(index) {
                    // Convert to string efficiently - XmlData likely implements Display
                    let xml_str = xml_data.to_string();
                    Ok(xml_str.into_pyobject(py)?.into_any().unbind())
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
    /// Ultra-fast column access using shared column map and direct Vec indexing
    pub fn __getitem__(&self, py: Python, key: Bound<PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(name) = key.extract::<String>() {
            // Access by name: O(1) hash lookup + O(1) Vec access
            if let Some(&index) = self.column_info.map.get(&name) {
                Ok(self.values[index].clone_ref(py))
            } else {
                Err(PyValueError::new_err(format!("Column '{}' not found", name)))
            }
        } else if let Ok(index) = key.extract::<usize>() {
            // Access by index: Direct O(1) Vec access - extremely fast!
            if let Some(value) = self.values.get(index) {
                Ok(value.clone_ref(py))
            } else {
                Err(PyValueError::new_err("Column index out of range"))
            }
        } else {
            Err(PyValueError::new_err("Key must be string or integer"))
        }
    }

    /// Get all column names from shared column info - returns slice to avoid cloning
    pub fn columns(&self) -> &[String] {
        &self.column_info.names
    }

    /// Get number of columns
    pub fn __len__(&self) -> usize {
        self.column_info.names.len()
    }

    /// Get a specific column value by name
    pub fn get(&self, py: Python, column: &str) -> PyResult<Py<PyAny>> {
        self.__getitem__(py, column.into_pyobject(py)?.into_any())
    }

    /// Get a value by column index
    pub fn get_by_index(&self, py: Python, index: usize) -> PyResult<Py<PyAny>> {
        self.__getitem__(py, index.into_pyobject(py)?.into_any())
    }

    /// Get all values as a list - optimized to minimize cloning
    pub fn values(&self, py: Python) -> PyResult<Py<pyo3::types::PyList>> {
        let py_list = pyo3::types::PyList::empty(py);
        for value in &self.values {
            py_list.append(value)?;
        }
        Ok(py_list.into())
    }

    /// Convert to dictionary - optimized with zip iterator
    pub fn to_dict(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        
        for (name, value) in self.column_info.names.iter().zip(self.values.iter()) {
            dict.set_item(name, value)?;
        }
        
        Ok(dict.into())
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!("FastRow with {} columns", self.column_info.names.len())
    }

    /// Detailed representation
    pub fn __repr__(&self) -> String {
        format!("FastRow(columns={:?})", self.column_info.names)
    }
}

/// Optimized execution result that can return either FastRow objects or affected count.
/// Now manages shared column information for memory efficiency.
#[pyclass(name = "FastExecutionResult")]
pub struct PyFastExecutionResult {
    rows: Option<Vec<PyFastRow>>,
    affected_rows: Option<u64>,
    // Shared column info for all rows in this result set
    column_info: Option<Arc<ColumnInfo>>,
    pub index: usize,
}

#[pymethods]
impl PyFastExecutionResult {
    /// Get the returned rows (if any) - zero-copy reference access
    pub fn rows(&self, py: Python) -> PyResult<Py<PyAny>> {
        match &self.rows {
            Some(rows) => {
                let py_list = pyo3::types::PyList::empty(py);
                for row in rows {
                    let py_row = Py::new(py, PyFastRow {
                        values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
                        column_info: Arc::clone(&row.column_info),
                    })?;
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
            column_info: None,
            index: 0,
        }
    }
    /// Fetch the next row or None
    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if let Some(rows) = &self.rows {
            if self.index >= rows.len() {
                return Ok(None);
            }
            let row = &rows[self.index];
            self.index += 1;
            Ok(Some(Py::new(py, PyFastRow {
                values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
                column_info: Arc::clone(&row.column_info),
            })?.into()))
        } else {
            Ok(None)
        }
    }
    

    /// Fetch up to `size` rows (default 1)
    pub fn fetchmany(&mut self, py: Python<'_>, size: Option<usize>) -> PyResult<Vec<Py<PyAny>>> {
        let n = size.unwrap_or(1);
        let mut out = Vec::with_capacity(n);
        if let Some(rows) = &self.rows {
            for _ in 0..n {
                if self.index >= rows.len() {
                    break;
                }
                let row = &rows[self.index];
                self.index += 1;
                out.push(Py::new(py, PyFastRow {
                    values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
                    column_info: Arc::clone(&row.column_info),
                })?.into());
            }
        }
        Ok(out)
    }

    /// Fetch all remaining rows
    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::new();
        if let Some(rows) = &self.rows {
            while self.index < rows.len() {
                let row = &rows[self.index];
                self.index += 1;
                out.push(Py::new(py, PyFastRow {
                    values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
                    column_info: Arc::clone(&row.column_info),
                })?.into()); // <-- HERE
            }
        }
        Ok(out)
    }
}

impl PyFastExecutionResult {
    /// Create a result with rows - efficient shared column info approach
    pub fn with_rows(tiberius_rows: Vec<tiberius::Row>, py: Python) -> PyResult<Self> {
        if tiberius_rows.is_empty() {
            return Ok(Self {
                rows: Some(Vec::new()),
                affected_rows: None,
                column_info: None,
                index: 0,
            });
        }

        let first_row = &tiberius_rows[0];
        let mut names = Vec::with_capacity(first_row.columns().len());
        let mut map = HashMap::with_capacity(first_row.columns().len());
        
        for (i, col) in first_row.columns().iter().enumerate() {
            let name = col.name().to_string();
            map.insert(name.clone(), i);
            names.push(name);
        }
        
        let column_info = Arc::new(ColumnInfo { names, map });

        let mut fast_rows = Vec::with_capacity(tiberius_rows.len());
        for row in tiberius_rows.into_iter() {
            fast_rows.push(PyFastRow::from_tiberius_row(row, py, Arc::clone(&column_info))?);
        }
        
        Ok(Self {
            rows: Some(fast_rows),
            affected_rows: None,
            column_info: Some(column_info),
            index: 0,
        })
    }
    
    /// Create a placeholder result that will have rows added later
    pub fn placeholder_for_rows() -> Self {
        Self {
            rows: None,
            affected_rows: None,
            column_info: None,
            index: 0,
        }
    }
    
    /// Set rows from Tiberius rows - efficient shared column info approach
    pub fn set_rows_from_tiberius(&mut self, tiberius_rows: Vec<tiberius::Row>, py: Python) -> PyResult<()> {
        if tiberius_rows.is_empty() {
            self.rows = Some(Vec::new());
            self.column_info = None;
            return Ok(());
        }

        // Create shared column info from the first row - optimized to avoid cloning
        let first_row = &tiberius_rows[0];
        let mut names = Vec::with_capacity(first_row.columns().len());
        let mut map = HashMap::with_capacity(first_row.columns().len());
        
        for (i, col) in first_row.columns().iter().enumerate() {
            let name = col.name().to_string();
            map.insert(name.clone(), i);
            names.push(name);
        }
        
        let column_info = Arc::new(ColumnInfo { names, map });

        let mut fast_rows = Vec::with_capacity(tiberius_rows.len());
        for row in tiberius_rows.into_iter() {
            fast_rows.push(PyFastRow::from_tiberius_row(row, py, Arc::clone(&column_info))?);
        }
        
        self.rows = Some(fast_rows);
        self.column_info = Some(column_info);
        Ok(())
    }
    
    /// Create a result with affected row count
    pub fn with_affected_count(count: u64) -> Self {
        Self {
            rows: None,
            affected_rows: Some(count),
            column_info: None,
            index: 0,
        }
    }
    }
