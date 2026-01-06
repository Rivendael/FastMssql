use crate::type_mapping;
use ahash::AHashMap as HashMap;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tiberius::{ColumnType, Row};
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

impl Clone for PyFastRow {
    fn clone(&self) -> Self {
        Python::attach(|py| PyFastRow {
            values: self.values.iter().map(|v| v.clone_ref(py)).collect(),
            column_info: Arc::clone(&self.column_info),
        })
    }
}

impl PyFastRow {
    /// Create a new PyFastRow from a Tiberius row and shared column info
    pub fn from_tiberius_row(row: Row, py: Python, column_info: Arc<ColumnInfo>) -> PyResult<Self> {
        // Eagerly convert all values in column order using cached column types
        let mut values = Vec::with_capacity(column_info.names.len());
        for i in 0..column_info.names.len() {
            let col_type = column_info.column_types.get(i).copied().ok_or_else(|| {
                PyValueError::new_err(format!("Column type not found for index {}", i))
            })?;
            let value = Self::extract_value_direct(&row, i, col_type, py)?;
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
    fn extract_value_direct(
        row: &Row,
        index: usize,
        col_type: ColumnType,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
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
                Err(PyValueError::new_err(format!(
                    "Column '{}' not found",
                    name
                )))
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

/// Helper to build column info from the first row
/// Caches both column names and types for efficient value conversion
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

    Arc::new(ColumnInfo {
        names,
        map,
        column_types,
    })
}

/// A streaming wrapper around a Tiberius QueryStream
/// Implements async iteration to fetch rows one at a time
#[pyclass(name = "QueryStream")]
pub struct PyQueryStream {
    // We can't store the actual QueryStream here because PyO3 classes need Send + Sync
    // Instead, we'll store rows as they're fetched and manage iteration state
    rows: Vec<PyFastRow>,
    column_info: Option<Arc<ColumnInfo>>,
    position: usize,
    is_complete: bool,
}

#[pymethods]
impl PyQueryStream {
    /// Return self for synchronous iteration protocol (for row in result:)
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get the next row in synchronous iteration
    /// Returns the next FastRow, or raises StopIteration when complete
    pub fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Py::new(py, row).map(|p| p.into_any())
        } else {
            // All rows have been iterated
            self.is_complete = true;
            Err(pyo3::exceptions::PyStopIteration::new_err(""))
        }
    }

    /// Load all remaining rows at once
    /// Returns a list of PyFastRow objects
    pub fn all(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let remaining = &self.rows[self.position..];
        let mut row_list = Vec::with_capacity(remaining.len());

        for row in remaining {
            let py_row = Py::new(py, row.clone())?;
            row_list.push(py_row.into_any());
        }

        self.position = self.rows.len();
        let py_list = pyo3::types::PyList::new(py, row_list)?;
        Ok(py_list.into())
    }

    /// Get the next N rows as a batch
    pub fn fetch(&mut self, py: Python<'_>, n: usize) -> PyResult<Py<PyAny>> {
        let end = std::cmp::min(self.position + n, self.rows.len());
        let batch = &self.rows[self.position..end];
        let mut row_list = Vec::with_capacity(batch.len());

        for row in batch {
            let py_row = Py::new(py, row.clone())?;
            row_list.push(py_row.into_any());
        }

        self.position = end;
        let py_list = pyo3::types::PyList::new(py, row_list)?;
        Ok(py_list.into())
    }

    /// Get column names
    pub fn columns(&self) -> PyResult<Vec<String>> {
        match &self.column_info {
            Some(info) => Ok(info.names.clone()),
            None => Err(PyValueError::new_err("No column information available")),
        }
    }

    /// Reset iteration to the beginning
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Get current position in the stream
    pub fn position(&self) -> usize {
        self.position
    }

    /// Get total number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Support for Python's len() builtin
    pub fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// Check if stream is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Backwards compatibility: check if stream has rows
    pub fn has_rows(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Backwards compatibility: get all rows at once (returns to beginning)
    pub fn rows(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Reset to beginning and return all rows
        self.position = 0;
        self.all(py)
    }

    /// Backwards compatibility: fetch one row
    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyFastRow>>> {
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Ok(Some(Py::new(py, row)?))
        } else {
            Ok(None)
        }
    }

    /// Backwards compatibility: fetch many rows
    pub fn fetchmany(&mut self, py: Python<'_>, n: usize) -> PyResult<Py<PyAny>> {
        let end = std::cmp::min(self.position + n, self.rows.len());
        let batch = &self.rows[self.position..end];
        let mut row_list = Vec::with_capacity(batch.len());

        for row in batch {
            let py_row = Py::new(py, row.clone())?;
            row_list.push(py_row.into_any());
        }

        self.position = end;
        let py_list = pyo3::types::PyList::new(py, row_list)?;
        Ok(py_list.into())
    }

    /// Backwards compatibility: fetch all rows
    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.all(py)
    }
}

impl PyQueryStream {
    /// Create a new QueryStream from Tiberius rows
    pub fn from_tiberius_rows(tiberius_rows: Vec<tiberius::Row>, py: Python) -> PyResult<Self> {
        if tiberius_rows.is_empty() {
            return Ok(PyQueryStream {
                rows: Vec::new(),
                column_info: None,
                position: 0,
                is_complete: false,
            });
        }

        // Create shared column info from the first row
        let first_row = &tiberius_rows[0];
        let column_info = build_column_info(first_row);

        let mut fast_rows = Vec::with_capacity(tiberius_rows.len());
        for row in tiberius_rows.into_iter() {
            fast_rows.push(PyFastRow::from_tiberius_row(
                row,
                py,
                Arc::clone(&column_info),
            )?);
        }

        Ok(PyQueryStream {
            rows: fast_rows,
            column_info: Some(column_info),
            position: 0,
            is_complete: false,
        })
    }
}
