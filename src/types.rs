use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyType};
use pyo3::prelude::*;
use tiberius::{Row, ColumnType};
use ahash::AHashMap as HashMap;
use std::sync::Arc;
use crate::type_mapping;
/// Holds shared column information for a result set to reduce memory usage.
/// This is shared across all `PyFastRow` instances in a result set.
#[derive(Debug)]
pub struct ColumnInfo {
    /// Ordered list of column names
    pub names: Vec<String>,
    /// Map from column name to its index for fast lookups
    pub map: HashMap<String, usize>,
    /// Cached column types (one per column) to avoid repeated lookups during value conversion
    pub column_types: Vec<ColumnType>,
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
        // Eagerly convert all values in column order using cached column types
        let mut values = Vec::with_capacity(column_info.names.len());
        for i in 0..column_info.names.len() {
            let value = Self::extract_value_direct(&row, i, column_info.column_types[i], py)?;
            values.push(value);
        }
        
        Ok(PyFastRow {
            values,
            column_info,
        })
    }

    /// Convert value directly from Tiberius to Python using centralized type mapping
    /// Uses cached column type to avoid repeated lookups
    #[inline]
    fn extract_value_direct(row: &Row, index: usize, col_type: ColumnType, py: Python) -> PyResult<Py<PyAny>> {
        type_mapping::sql_to_python(row, index, col_type, py)
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

impl PyFastExecutionResult {
    /// Build column info from the first row (helper to avoid duplication)
    /// Caches both column names and types for efficient value conversion
    #[inline]
    fn build_column_info(first_row: &Row) -> Arc<ColumnInfo> {
        let mut names = Vec::with_capacity(first_row.columns().len());
        let mut column_types = Vec::with_capacity(first_row.columns().len());
        let mut map = HashMap::with_capacity(first_row.columns().len());
        
        for (i, col) in first_row.columns().iter().enumerate() {
            let name = col.name().to_string();
            map.insert(name.clone(), i);
            names.push(name);
            column_types.push(col.column_type());
        }
        
        Arc::new(ColumnInfo { names, map, column_types })
    }

    /// Convert a PyFastRow to a Python object (helper to avoid duplication)
    #[inline]
    fn convert_row_to_py(&self, py: Python, row: &PyFastRow) -> PyResult<Py<PyAny>> {
        let result = PyFastRow {
            values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
            column_info: Arc::clone(&row.column_info),
        };
        Py::new(py, result).map(|p| p.into())
    }
}

#[pymethods]
impl PyFastExecutionResult {
    /// Get the returned rows (if any) - optimized with pre-allocation and single pass
    pub fn rows(&self, py: Python) -> PyResult<Py<PyAny>> {
        match &self.rows {
            Some(rows) => {
                // Pre-allocate list with exact size to avoid resizing
                let mut row_list = Vec::with_capacity(rows.len());
                
                // Batch convert all rows
                for row in rows {
                    let py_row = Py::new(py, PyFastRow {
                        values: row.values.iter().map(|v| v.clone_ref(py)).collect(),
                        column_info: Arc::clone(&row.column_info),
                    })?;
                    row_list.push(py_row.into_any());
                }
                
                // Create list once with all items
                let py_list = pyo3::types::PyList::new(py, row_list)?;
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
            Ok(Some(self.convert_row_to_py(py, row)?))
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
                out.push(self.convert_row_to_py(py, row)?);
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
                out.push(self.convert_row_to_py(py, row)?);
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
        let column_info = Self::build_column_info(first_row);

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
        let column_info = Self::build_column_info(first_row);

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
