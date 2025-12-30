use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple, PySet, PyFrozenSet};
use tiberius::{Row, ColumnType};
use chrono::{Datelike, Timelike};

#[inline(always)]
fn handle_int4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_int8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_int1(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<u8, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_int2(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i16, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_float8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_float4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f32, usize>(index) {
        Ok(Some(val)) => Ok((val as f64).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_nvarchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_varchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_bit(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<bool, usize>(index) {
        Ok(Some(val)) => {
            let int_val = if val { 1i32 } else { 0i32 };
            Ok(int_val.into_pyobject(py)?.into_any().unbind())
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_binary(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&[u8], usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_money(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i64, usize>(index) {
        Ok(Some(val)) => {
            let money_val = (val as f64) / 10000.0;
            Ok(money_val.into_pyobject(py)?.into_any().unbind())
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_money4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => {
            let money_val = (val as f64) / 10000.0;
            Ok(money_val.into_pyobject(py)?.into_any().unbind())
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_decimal(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<tiberius::numeric::Numeric, usize>(index) {
        Ok(Some(numeric)) => {
            let float_val: f64 = numeric.into();
            Ok(float_val.into_pyobject(py)?.into_any().unbind())
        },
        _ => Ok(py.None())
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
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_date(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveDate, usize>(index) {
        Ok(Some(val)) => {
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
        },
        _ => Ok(py.None())
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
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_uuid(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<uuid::Uuid, usize>(index) {
        Ok(Some(val)) => {
            let uuid_str = format!("{}", val);
            Ok(uuid_str.into_pyobject(py)?.into_any().unbind())
        },
        _ => Ok(py.None())
    }
}

#[inline(always)]
fn handle_xml(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    if let Ok(Some(xml_data)) = row.try_get::<&tiberius::xml::XmlData, usize>(index) {
        let xml_str = xml_data.to_string();
        Ok(xml_str.into_pyobject(py)?.into_any().unbind())
    } else {
        Ok(py.None())
    }
}

#[inline(always)]
fn handle_fallback(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        _ => Ok(py.None())
    }
}

/// Convert a SQL Server column value from Tiberius to Python
/// 
pub fn sql_to_python(row: &Row, index: usize, col_type: ColumnType, py: Python) -> PyResult<Py<PyAny>> {
    // Dispatch to specialized handlers - better branch prediction than giant match
    match col_type {
        ColumnType::Int4 => handle_int4(row, index, py),
        ColumnType::Int8 => handle_int8(row, index, py),
        ColumnType::Int1 => handle_int1(row, index, py),
        ColumnType::Int2 => handle_int2(row, index, py),
        ColumnType::Float8 => handle_float8(row, index, py),
        ColumnType::Float4 => handle_float4(row, index, py),
        ColumnType::NVarchar => handle_nvarchar(row, index, py),
        ColumnType::BigVarChar | ColumnType::NChar | ColumnType::BigChar => handle_varchar(row, index, py),
        ColumnType::Bit | ColumnType::Bitn => handle_bit(row, index, py),
        ColumnType::BigBinary | ColumnType::BigVarBin | ColumnType::Image => handle_binary(row, index, py),
        ColumnType::Money => handle_money(row, index, py),
        ColumnType::Money4 => handle_money4(row, index, py),
        ColumnType::Decimaln | ColumnType::Numericn => handle_decimal(row, index, py),
        ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime2 => handle_datetime(row, index, py),
        ColumnType::Daten => handle_date(row, index, py),
        ColumnType::Timen => handle_time(row, index, py),
        ColumnType::DatetimeOffsetn => handle_datetimeoffset(row, index, py),
        ColumnType::Guid => handle_uuid(row, index, py),
        ColumnType::Xml => handle_xml(row, index, py),
        // Fallback for any unknown types
        _ => handle_fallback(row, index, py),
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


