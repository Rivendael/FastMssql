use crate::type_mapping;
use crate::types::ColumnInfo;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tiberius::ColumnType;

/// Cache of frequently-used Arrow type objects to avoid repeated PyArrow imports and object creation
struct ArrowTypeCache {
    int64: Py<PyAny>,
    float64: Py<PyAny>,
    string: Py<PyAny>,
    bool_: Py<PyAny>,
    binary: Py<PyAny>,
    null: Py<PyAny>,
    date32: Py<PyAny>,
    /// Reference to pyarrow module for creating complex types (Decimal, Timestamp, etc.)
    pyarrow: Py<PyModule>,
}

impl ArrowTypeCache {
    fn new(py: Python) -> PyResult<Self> {
        let pyarrow = py.import("pyarrow")?;
        Ok(ArrowTypeCache {
            int64: pyarrow.getattr("int64")?.call0()?.unbind(),
            float64: pyarrow.getattr("float64")?.call0()?.unbind(),
            string: pyarrow.getattr("string")?.call0()?.unbind(),
            bool_: pyarrow.getattr("bool_")?.call0()?.unbind(),
            binary: pyarrow.getattr("binary")?.call0()?.unbind(),
            null: pyarrow.getattr("null")?.call0()?.unbind(),
            date32: pyarrow.getattr("date32")?.call0()?.unbind(),
            pyarrow: pyarrow.unbind(),
        })
    }
}

/// Maps SQL column types to corresponding Arrow types, using a shared cache to minimize imports.
/// This avoids repeated PyArrow module imports and provides consistent type mapping.
fn get_arrow_type_with_cache(
    col_type: ColumnType,
    cache: &ArrowTypeCache,
    py: Python,
) -> PyResult<Py<PyAny>> {
    let arrow_type = match col_type {
        // Integer types: all mapped to int64 for consistency
        ColumnType::Int1
        | ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Intn => cache.int64.clone_ref(py),

        // Floating point types
        ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => cache.float64.clone_ref(py),

        // String types
        ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::Text
        | ColumnType::NText
        | ColumnType::Guid
        | ColumnType::Xml => cache.string.clone_ref(py),

        // Boolean type
        ColumnType::Bit | ColumnType::Bitn => cache.bool_.clone_ref(py),

        // Money types: decimal128 with precision 38, scale 4
        ColumnType::Money | ColumnType::Money4 => {
            let pyarrow = cache.pyarrow.bind(py);
            let args = pyo3::types::PyTuple::new(py, &[38i32, 4i32])?;
            let decimal_method = pyarrow.getattr("decimal128")?;
            decimal_method.call1(args)?.unbind()
        }

        // Decimal/Numeric types: decimal128 with precision 38, scale 10
        ColumnType::Decimaln | ColumnType::Numericn => {
            let pyarrow = cache.pyarrow.bind(py);
            let args = pyo3::types::PyTuple::new(py, &[38i32, 10i32])?;
            let decimal_method = pyarrow.getattr("decimal128")?;
            decimal_method.call1(args)?.unbind()
        }

        // DateTime types: timestamp with microsecond precision
        ColumnType::Datetime
        | ColumnType::Datetimen
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::DatetimeOffsetn => {
            let pyarrow = cache.pyarrow.bind(py);
            let timestamp_method = pyarrow.getattr("timestamp")?;
            timestamp_method.call1(("us",))?.unbind()
        }

        // Date type: date32 (days since UNIX epoch)
        ColumnType::Daten => cache.date32.clone_ref(py),

        // Time type: time64 with microsecond precision
        ColumnType::Timen => {
            let pyarrow = cache.pyarrow.bind(py);
            let time_method = pyarrow.getattr("time64")?;
            time_method.call1(("us",))?.unbind()
        }

        // Binary types
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => {
            cache.binary.clone_ref(py)
        }

        // Special types: stored as binary
        ColumnType::SSVariant | ColumnType::Udt => cache.binary.clone_ref(py),

        // Null type
        ColumnType::Null => cache.null.clone_ref(py),
    };

    Ok(arrow_type)
}

/// Converts SQL query results into Arrow arrays, one per column.
///
/// This function handles the critical conversion of tiberius Row data to PyArrow arrays.
/// Key optimizations:
/// - ArrowTypeCache reused across all columns (single pyarrow import)
/// - Pre-allocated vectors sized to row count
/// - Type casting explicitly used only for Money, Decimal, and DateTime (types requiring schema info)
/// - Clear error messages showing problematic column for debugging
///
/// # Arguments:
/// - `rows`: Raw SQL result rows
/// - `column_info`: Column metadata (names, types)
/// - `py`: Python interpreter context
///
/// # Returns: Vector of PyArrow arrays (one per column), suitable for `arrow_arrays_to_pyarrow_table()`
pub fn build_arrow_columns(
    rows: &[Option<tiberius::Row>],
    column_info: &Arc<ColumnInfo>,
    py: Python,
) -> PyResult<Vec<Py<PyAny>>> {
    let pyarrow = py.import("pyarrow")?;
    let array_class = pyarrow.getattr("array")?;
    let cache = ArrowTypeCache::new(py)?;

    let num_columns = column_info.names.len();
    let num_rows = rows.len();
    let mut columns = Vec::with_capacity(num_columns);

    // Handle empty result sets: create properly-typed empty arrays
    if num_rows == 0 {
        for (col_idx, col_type) in column_info.column_types.iter().enumerate() {
            let arrow_type = get_arrow_type_with_cache(*col_type, &cache, py).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to determine Arrow type for column {}: {}",
                    col_idx, e
                ))
            })?;
            let empty_list = pyo3::types::PyList::empty(py);
            let array = array_class
                .call((empty_list, arrow_type), None)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to create empty Array for column {}: {}",
                        col_idx, e
                    ))
                })?;
            columns.push(array.unbind());
        }
        return Ok(columns);
    }

    // Process each column
    for col_idx in 0..num_columns {
        let col_type = column_info.column_types[col_idx];
        let mut column_values = Vec::with_capacity(num_rows);

        // Convert values for this column across all rows
        for (row_idx, row_opt) in rows.iter().enumerate() {
            let value = if let Some(row) = row_opt {
                type_mapping::sql_to_python(row, col_idx, col_type, py).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Column '{}' (idx {}), Row {}: {}",
                        &column_info.names[col_idx], col_idx, row_idx, e
                    ))
                })?
            } else {
                py.None()
            };
            column_values.push(value);
        }

        let py_list = pyo3::types::PyList::new(py, &column_values).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create Python list for column {}: {}",
                col_idx, e
            ))
        })?;
        let arrow_type = get_arrow_type_with_cache(col_type, &cache, py).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to determine Arrow type for column {}: {}",
                col_idx, e
            ))
        })?;

        // Use explicit type for complex types that require schema specification.
        // PyArrow can infer types for simple types (int, float, string, bool) but needs
        // explicit schemas for Decimal, Timestamp, and similar types to ensure correct precision/scale.
        let array = match col_type {
            ColumnType::Money
            | ColumnType::Money4
            | ColumnType::Decimaln
            | ColumnType::Numericn
            | ColumnType::Datetime
            | ColumnType::Datetimen
            | ColumnType::Datetime2
            | ColumnType::Datetime4
            | ColumnType::DatetimeOffsetn => {
                // Explicit type required for proper schema specification
                array_class.call((py_list, &arrow_type), None)
            }
            _ => {
                // PyArrow can reliably infer types for these
                array_class.call1((py_list,))
            }
        }
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create Arrow array for column {}: {}",
                col_idx, e
            ))
        })?;

        columns.push(array.unbind());
    }

    Ok(columns)
}

/// Constructs a PyArrow Table from pre-built columns and names.
///
/// PyArrow Table construction maps column names to Arrow arrays. This function
/// handles the final assembly step after column conversion.
///
/// # Arguments:
/// - `column_names`: Ordered list of column names from query results
/// - `arrays`: Arrow arrays (one per column, order must match names)
/// - `py`: Python interpreter context
///
/// # Returns: A PyArrow Table object suitable for use in Python
pub fn arrow_arrays_to_pyarrow_table(
    column_names: &[String],
    arrays: Vec<Py<PyAny>>,
    py: Python,
) -> PyResult<Py<PyAny>> {
    let pyarrow = py.import("pyarrow")?;
    let table_class = pyarrow.getattr("table")?;

    let dict = PyDict::new(py);
    for (name, array) in column_names.iter().zip(arrays.iter()) {
        dict.set_item(name, array).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to add column '{}' to PyArrow table: {}",
                name, e
            ))
        })?;
    }

    let table = table_class.call1((dict,)).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create PyArrow Table: {}", e))
    })?;
    Ok(table.unbind())
}
