use chrono::{Datelike, Timelike};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyFrozenSet, PyList, PySet, PyString, PyTuple};
use tiberius::{ColumnType, Row};

#[inline(always)]
fn handle_int4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT4: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_int8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT8: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_int1(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<u8, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT1: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_int2(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i16, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT2: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_float8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to FLOAT8: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_float4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f32, usize>(index) {
        Ok(Some(val)) => Ok((val as f64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to FLOAT4: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_nvarchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to NVARCHAR: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_varchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to VARCHAR: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_bit(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<bool, usize>(index) {
        Ok(Some(val)) => {
            let int_val = if val { 1i32 } else { 0i32 };
            Ok(int_val.into_pyobject(py)?.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to BIT: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_binary(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&[u8], usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to BINARY: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_money(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => {
            // Convert to Decimal to preserve precision for financial data
            let decimal_str = val.to_string();
            let decimal_class = py.import("decimal")?.getattr("Decimal")?;
            Ok(decimal_class.call1((decimal_str,))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to MONEY: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_money4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => {
            // Convert to Decimal to preserve precision for financial data
            let decimal_str = val.to_string();
            let decimal_class = py.import("decimal")?.getattr("Decimal")?;
            Ok(decimal_class.call1((decimal_str,))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to MONEY4: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_decimal(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<tiberius::numeric::Numeric, usize>(index) {
        Ok(Some(numeric)) => {
            // Convert to Decimal to preserve precision for financial data
            let decimal_str = numeric.to_string();
            let decimal_class = py.import("decimal")?.getattr("Decimal")?;
            Ok(decimal_class.call1((decimal_str,))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DECIMAL: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_datetime(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveDateTime, usize>(index) {
        Ok(Some(val)) => {
            let dt = pyo3::types::PyDateTime::new(
                py,
                val.year(),
                val.month() as u8,
                val.day() as u8,
                val.hour() as u8,
                val.minute() as u8,
                val.second() as u8,
                val.nanosecond() / 1000,
                None,
            )?;
            Ok(dt.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATETIME: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_date(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveDate, usize>(index) {
        Ok(Some(val)) => {
            let date =
                pyo3::types::PyDate::new(py, val.year(), val.month() as u8, val.day() as u8)?;
            Ok(date.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATE: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_time(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveTime, usize>(index) {
        Ok(Some(val)) => {
            let time = pyo3::types::PyTime::new(
                py,
                val.hour() as u8,
                val.minute() as u8,
                val.second() as u8,
                val.nanosecond() / 1000,
                None,
            )?;
            Ok(time.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to TIME: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_datetimeoffset(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::DateTime<chrono::Utc>, usize>(index) {
        Ok(Some(val)) => {
            let dt = pyo3::types::PyDateTime::new(
                py,
                val.year(),
                val.month() as u8,
                val.day() as u8,
                val.hour() as u8,
                val.minute() as u8,
                val.second() as u8,
                val.nanosecond() / 1000,
                None,
            )?;
            Ok(dt.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATETIMEOFFSET: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_uuid(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<uuid::Uuid, usize>(index) {
        Ok(Some(val)) => {
            let uuid_str = val.hyphenated().to_string();
            Ok(uuid_str.into_pyobject(py)?.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to UUID: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_xml(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&tiberius::xml::XmlData, usize>(index) {
        Ok(Some(xml_data)) => {
            let xml_str = xml_data.to_string();
            Ok(xml_str.into_pyobject(py)?.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to XML: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_nchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to NCHAR: {}",
            index, e
        ))),
    }
}

#[inline(always)]
fn handle_fallback(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(e) => Err(PyValueError::new_err(format!(
            "Failed to convert column {}: {}",
            index, e
        ))),
    }
}

/// Convert a SQL Server column value from Tiberius to Python
///
pub fn sql_to_python(
    row: &Row,
    index: usize,
    col_type: ColumnType,
    py: Python,
) -> PyResult<Py<PyAny>> {
    // Dispatch to specialized handlers - better branch prediction than giant match
    match col_type {
        ColumnType::Int4 => handle_int4(row, index, py),
        ColumnType::Int8 => handle_int8(row, index, py),
        ColumnType::Int1 => handle_int1(row, index, py),
        ColumnType::Int2 => handle_int2(row, index, py),
        ColumnType::Intn => handle_fallback(row, index, py), // Variable-length integer
        ColumnType::Float8 => handle_float8(row, index, py),
        ColumnType::Float4 => handle_float4(row, index, py),
        ColumnType::Floatn => handle_fallback(row, index, py), // Variable-length float
        ColumnType::NVarchar => handle_nvarchar(row, index, py),
        ColumnType::NChar => handle_nchar(row, index, py),
        ColumnType::BigVarChar | ColumnType::BigChar => handle_varchar(row, index, py),
        ColumnType::Text => handle_varchar(row, index, py), // Legacy text type
        ColumnType::NText => handle_nvarchar(row, index, py), // Legacy ntext type
        ColumnType::Image => handle_binary(row, index, py), // Legacy binary type
        ColumnType::Bit | ColumnType::Bitn => handle_bit(row, index, py),
        ColumnType::Money => handle_money(row, index, py),
        ColumnType::Money4 => handle_money4(row, index, py),
        ColumnType::Decimaln | ColumnType::Numericn => handle_decimal(row, index, py),
        ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime2 => {
            handle_datetime(row, index, py)
        }
        ColumnType::Datetime4 => handle_datetime(row, index, py), // 32-bit datetime
        ColumnType::Daten => handle_date(row, index, py),
        ColumnType::Timen => handle_time(row, index, py),
        ColumnType::DatetimeOffsetn => handle_datetimeoffset(row, index, py),
        ColumnType::Guid => handle_uuid(row, index, py),
        ColumnType::Xml => handle_xml(row, index, py),
        ColumnType::SSVariant => handle_fallback(row, index, py), // SQL_VARIANT type - use fallback
        ColumnType::BigVarBin => handle_binary(row, index, py),   // Variable binary data
        ColumnType::BigBinary => handle_binary(row, index, py),   // Fixed-length binary
        ColumnType::Udt => handle_fallback(row, index, py),       // User-defined type
        ColumnType::Null => Ok(py.None()),                        // NULL type
    }
}

/// Check if a Python object is an iterable that should be expanded for parameters
///
/// Returns true for lists, tuples, sets, etc., but false for strings and bytes
/// which should be treated as single values.
pub fn is_expandable_iterable(obj: &Bound<PyAny>) -> PyResult<bool> {
    // Fast path: Don't expand strings or bytes
    if obj.is_instance_of::<PyString>() || obj.is_instance_of::<PyBytes>() {
        return Ok(false);
    }

    // Check specific types that should be expanded
    if obj.cast::<PyList>().is_ok() {
        return Ok(true);
    }
    if obj.cast::<PyTuple>().is_ok() {
        return Ok(true);
    }
    if obj.cast::<PySet>().is_ok() {
        return Ok(true);
    }
    if obj.cast::<PyFrozenSet>().is_ok() {
        return Ok(true);
    }

    // Fallback: Check if it has __iter__ method (for custom iterables)
    Ok(obj.hasattr("__iter__")?)
}
