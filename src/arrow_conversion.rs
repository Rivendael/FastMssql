use crate::type_mapping;
use crate::types::ColumnInfo;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tiberius::ColumnType;

struct ArrowTypeCache {
    int64: Py<PyAny>,
    float64: Py<PyAny>,
    string: Py<PyAny>,
    bool_: Py<PyAny>,
    binary: Py<PyAny>,
    null: Py<PyAny>,
    date32: Py<PyAny>,
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
        })
    }
}

fn get_arrow_type_with_cache(
    col_type: ColumnType,
    cache: &ArrowTypeCache,
    py: Python,
) -> PyResult<Py<PyAny>> {
    let arrow_type = match col_type {
        ColumnType::Int1
        | ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Intn => cache.int64.clone_ref(py),

        ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => cache.float64.clone_ref(py),

        ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::Text
        | ColumnType::NText
        | ColumnType::Guid
        | ColumnType::Xml => cache.string.clone_ref(py),

        ColumnType::Bit | ColumnType::Bitn => cache.bool_.clone_ref(py),

        ColumnType::Money | ColumnType::Money4 => {
            // decimal128(38, 4)
            let pyarrow = py.import("pyarrow")?;
            let args = pyo3::types::PyTuple::new(py, &[38i32, 4i32])?;
            let decimal_method = pyarrow.getattr("decimal128")?;
            decimal_method.call1(args)?.unbind()
        }

        ColumnType::Decimaln | ColumnType::Numericn => {
            // decimal128(38, 10)
            let pyarrow = py.import("pyarrow")?;
            let args = pyo3::types::PyTuple::new(py, &[38i32, 10i32])?;
            let decimal_method = pyarrow.getattr("decimal128")?;
            decimal_method.call1(args)?.unbind()
        }

        ColumnType::Datetime
        | ColumnType::Datetimen
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::DatetimeOffsetn => {
            // timestamp('us')
            let pyarrow = py.import("pyarrow")?;
            let timestamp_method = pyarrow.getattr("timestamp")?;
            timestamp_method.call1(("us",))?.unbind()
        }

        ColumnType::Daten => cache.date32.clone_ref(py),

        ColumnType::Timen => {
            // time64('us')
            let pyarrow = py.import("pyarrow")?;
            let time_method = pyarrow.getattr("time64")?;
            time_method.call1(("us",))?.unbind()
        }

        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => {
            cache.binary.clone_ref(py)
        }

        ColumnType::SSVariant | ColumnType::Udt => cache.binary.clone_ref(py),

        ColumnType::Null => cache.null.clone_ref(py),
    };

    Ok(arrow_type)
}

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

    if num_rows == 0 {
        for col_type in &column_info.column_types {
            let arrow_type = get_arrow_type_with_cache(*col_type, &cache, py)?;
            let empty_list = pyo3::types::PyList::empty(py);
            let array = array_class.call((empty_list, arrow_type), None)?;
            columns.push(array.unbind());
        }
        return Ok(columns);
    }

    for col_idx in 0..num_columns {
        let col_type = column_info.column_types[col_idx];
        let mut column_values = Vec::with_capacity(num_rows);

        for row_opt in rows {
            if let Some(row) = row_opt {
                let value = type_mapping::sql_to_python(row, col_idx, col_type, py)?;
                column_values.push(value);
            } else {
                column_values.push(py.None());
            }
        }

        let py_list = pyo3::types::PyList::new(py, &column_values)?;
        let arrow_type = get_arrow_type_with_cache(col_type, &cache, py)?;

        // Use explicit type casting only for Money and DateTime types that need it
        // This avoids an extra type check for common types
        let array = match col_type {
            ColumnType::Money
            | ColumnType::Money4
            | ColumnType::Datetime
            | ColumnType::Datetimen
            | ColumnType::Datetime2
            | ColumnType::Datetime4
            | ColumnType::DatetimeOffsetn => array_class.call((py_list, &arrow_type), None)?,
            _ => {
                // For other types, pyarrow can infer
                array_class.call1((py_list,))?
            }
        };

        columns.push(array.unbind());
    }

    Ok(columns)
}

pub fn arrow_arrays_to_pyarrow_table(
    column_names: &[String],
    arrays: Vec<Py<PyAny>>,
    py: Python,
) -> PyResult<Py<PyAny>> {
    let pyarrow = py.import("pyarrow")?;
    let table_class = pyarrow.getattr("table")?;

    let dict = PyDict::new(py);
    for (name, array) in column_names.iter().zip(arrays.iter()) {
        dict.set_item(name, array)?;
    }

    let table = table_class.call1((dict,))?;
    Ok(table.unbind())
}
