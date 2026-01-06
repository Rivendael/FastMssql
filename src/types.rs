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
            let col_type = column_info
                .column_types
                .get(i)
                .copied()
                .ok_or_else(|| PyValueError::new_err("Column type not found"))?;
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
        // Try string extraction first (most common case)
        if let Ok(name) = key.extract::<&str>() {
            // Access by name: O(1) hash lookup + O(1) Vec access
            if let Some(&index) = self.column_info.map.get(name) {
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
        Ok(pyo3::types::PyList::new(py, &self.values)?.into())
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

    for col in first_row.columns().iter() {
        let name = col.name().to_string();
        names.push(name);
        column_types.push(col.column_type());
    }

    // Build map after names are finalized to avoid clone
    for (i, name) in names.iter().enumerate() {
        map.insert(name.clone(), i);
    }

    Arc::new(ColumnInfo {
        names,
        map,
        column_types,
    })
}

/// A streaming wrapper around a Tiberius QueryStream
/// Implements async iteration to fetch rows one at a time
/// Lazy conversion: stores raw rows, converts to Python on-demand, caches for reset()
#[pyclass(name = "QueryStream")]
pub struct PyQueryStream {
    // Store raw Tiberius rows in Option (Row doesn't impl Clone, so we take() on first access)
    tiberius_rows: Vec<Option<Row>>,
    // Cache of converted rows (parallel to tiberius_rows, None = not yet converted)
    converted_cache: Vec<Option<PyFastRow>>,
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
        if self.position < self.tiberius_rows.len() {
            let fast_row = if let Some(cached) = &self.converted_cache[self.position] {
                cached.clone()
            } else {
                let row = self.tiberius_rows[self.position]
                    .take()
                    .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                let column_info = self
                    .column_info
                    .as_ref()
                    .ok_or_else(|| PyValueError::new_err("No column info"))?;
                let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                self.converted_cache[self.position] = Some(fast_row.clone());
                fast_row
            };
            self.position += 1;
            Py::new(py, fast_row).map(|p| p.into_any())
        } else {
            // All rows have been iterated
            self.is_complete = true;
            Err(pyo3::exceptions::PyStopIteration::new_err(""))
        }
    }

    /// Get a row by index or a slice of rows
    /// Supports negative indexing and slicing: result[0], result[-1], result[5:10]
    pub fn __getitem__(&mut self, py: Python<'_>, key: Bound<PyAny>) -> PyResult<Py<PyAny>> {
        // Handle slice
        if let Ok(slice) = key.cast::<pyo3::types::PySlice>() {
            let indices = slice.indices(self.tiberius_rows.len() as isize)?;
            let start = indices.start as usize;
            let stop = indices.stop as usize;
            let step = indices.step;

            if step != 1 {
                return Err(PyValueError::new_err("Slice step must be 1"));
            }

            // Handle empty slice - return empty list immediately
            if start >= stop || self.tiberius_rows.is_empty() {
                return Ok(pyo3::types::PyList::empty(py).into());
            }

            let mut row_list = Vec::with_capacity(stop - start);
            let column_info = self
                .column_info
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("No column info"))?;

            for i in start..stop {
                let fast_row = if let Some(cached) = &self.converted_cache[i] {
                    cached.clone()
                } else {
                    let row = self.tiberius_rows[i]
                        .take()
                        .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                    let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                    self.converted_cache[i] = Some(fast_row.clone());
                    fast_row
                };
                row_list.push(Py::new(py, fast_row)?.into_any());
            }

            let py_list = pyo3::types::PyList::new(py, row_list)?;
            return Ok(py_list.into());
        }

        // Handle single index
        if let Ok(index) = key.extract::<isize>() {
            let len = self.tiberius_rows.len() as isize;
            let actual_index = if index < 0 {
                (len + index) as usize
            } else {
                index as usize
            };

            if actual_index >= self.tiberius_rows.len() {
                return Err(pyo3::exceptions::PyIndexError::new_err(
                    "Index out of range",
                ));
            }

            let fast_row = if let Some(cached) = &self.converted_cache[actual_index] {
                cached.clone()
            } else {
                let row = self.tiberius_rows[actual_index]
                    .take()
                    .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                let column_info = self
                    .column_info
                    .as_ref()
                    .ok_or_else(|| PyValueError::new_err("No column info"))?;
                let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                self.converted_cache[actual_index] = Some(fast_row.clone());
                fast_row
            };

            return Py::new(py, fast_row).map(|p| p.into_any());
        }

        Err(PyValueError::new_err("Index must be an integer or slice"))
    }

    /// Load all remaining rows at once
    /// Returns a list of PyFastRow objects
    pub fn all(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let remaining_count = self.tiberius_rows.len() - self.position;
        let mut row_list = Vec::with_capacity(remaining_count);

        if remaining_count == 0 {
            return Ok(pyo3::types::PyList::empty(py).into());
        }

        let column_info = self
            .column_info
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("No column info"))?;

        for i in self.position..self.tiberius_rows.len() {
            let fast_row = if let Some(cached) = &self.converted_cache[i] {
                cached.clone()
            } else {
                let row = self.tiberius_rows[i]
                    .take()
                    .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                self.converted_cache[i] = Some(fast_row.clone());
                fast_row
            };
            let py_row = Py::new(py, fast_row)?;
            row_list.push(py_row.into_any());
        }

        self.position = self.tiberius_rows.len();
        let py_list = pyo3::types::PyList::new(py, row_list)?;
        Ok(py_list.into())
    }

    /// Get the next N rows as a batch
    pub fn fetch(&mut self, py: Python<'_>, n: usize) -> PyResult<Py<PyAny>> {
        let end = std::cmp::min(self.position + n, self.tiberius_rows.len());
        let batch_size = end - self.position;
        let mut row_list = Vec::with_capacity(batch_size);

        if batch_size == 0 {
            return Ok(pyo3::types::PyList::empty(py).into());
        }

        let column_info = self
            .column_info
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("No column info"))?;

        for i in self.position..end {
            let fast_row = if let Some(cached) = &self.converted_cache[i] {
                cached.clone()
            } else {
                let row = self.tiberius_rows[i]
                    .take()
                    .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                self.converted_cache[i] = Some(fast_row.clone());
                fast_row
            };
            let py_row = Py::new(py, fast_row)?;
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
        self.tiberius_rows.len()
    }

    /// Support for Python's len() builtin
    pub fn __len__(&self) -> usize {
        self.tiberius_rows.len()
    }

    /// Check if stream is empty
    pub fn is_empty(&self) -> bool {
        self.tiberius_rows.is_empty()
    }

    /// Backwards compatibility: check if stream has rows
    pub fn has_rows(&self) -> bool {
        !self.tiberius_rows.is_empty()
    }

    /// Backwards compatibility: get all rows at once (returns to beginning)
    pub fn rows(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Reset to beginning and return all rows
        self.position = 0;
        self.all(py)
    }

    /// Backwards compatibility: fetch one row
    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyFastRow>>> {
        if self.position < self.tiberius_rows.len() {
            let fast_row = if let Some(cached) = &self.converted_cache[self.position] {
                cached.clone()
            } else {
                let row = self.tiberius_rows[self.position]
                    .take()
                    .ok_or_else(|| PyValueError::new_err("Row already consumed"))?;
                let column_info = self
                    .column_info
                    .as_ref()
                    .ok_or_else(|| PyValueError::new_err("No column info"))?;
                let fast_row = PyFastRow::from_tiberius_row(row, py, Arc::clone(column_info))?;
                self.converted_cache[self.position] = Some(fast_row.clone());
                fast_row
            };
            self.position += 1;
            Ok(Some(Py::new(py, fast_row)?))
        } else {
            Ok(None)
        }
    }

    /// Backwards compatibility: fetch many rows
    pub fn fetchmany(&mut self, py: Python<'_>, n: usize) -> PyResult<Py<PyAny>> {
        self.fetch(py, n)
    }

    /// Backwards compatibility: fetch all rows
    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.all(py)
    }
}

impl PyQueryStream {
    /// Create a new QueryStream from Tiberius rows
    /// LAZY: stores raw rows, NO Python conversion (minimal GIL hold)
    /// Rows converted on-demand during iteration and cached for reset()
    pub fn from_tiberius_rows(tiberius_rows: Vec<tiberius::Row>, _py: Python) -> PyResult<Self> {
        if tiberius_rows.is_empty() {
            return Ok(PyQueryStream {
                tiberius_rows: Vec::new(),
                converted_cache: Vec::new(),
                column_info: None,
                position: 0,
                is_complete: false,
            });
        }

        // Create shared column info from the first row
        let first_row = &tiberius_rows[0];
        let column_info = build_column_info(first_row);

        let row_count = tiberius_rows.len();
        // Initialize cache - all None (not yet converted)
        let converted_cache: Vec<Option<PyFastRow>> = vec![None; row_count];

        // Wrap rows in Some() since Row doesn't impl Clone
        let wrapped_rows: Vec<Option<Row>> = tiberius_rows.into_iter().map(Some).collect();

        // Store raw rows - NO Python conversion!
        // This is the key: from_tiberius_rows() returns instantly
        Ok(PyQueryStream {
            tiberius_rows: wrapped_rows,
            converted_cache,
            column_info: Some(column_info),
            position: 0,
            is_complete: false,
        })
    }
}
