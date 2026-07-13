use std::sync::OnceLock;

use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyFrozenSet, PyList, PySet, PyString, PyTuple};
use pyo3::{IntoPyObjectExt, Py, PyAny, prelude::*};
use tiberius::{ColumnType, Row};

/// Cached handle to `decimal.Decimal` — imported once, reused for every row.
/// Stored as `Option` to allow initialization via `get_or_init()` with fallible closure.
static DECIMAL_CLASS: OnceLock<Option<Py<PyAny>>> = OnceLock::new();

/// Return a `Bound` reference to `decimal.Decimal`, initializing the cache on
/// the very first call and simply re-binding on every subsequent call.
#[inline]
fn get_decimal_class(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    let cls = DECIMAL_CLASS
        .get_or_init(|| {
            py.import("decimal")
                .and_then(|m| m.getattr("Decimal"))
                .map(|d| d.unbind())
                .ok()
        })
        .as_ref()
        .ok_or_else(|| PyValueError::new_err("Failed to initialize decimal.Decimal"))?;
    Ok(cls.bind(py))
}

/// Macro to eliminate boilerplate for identical scalar type conversions.
macro_rules! impl_handle_scalar {
    ($name:ident, $t:ty, $lbl:expr) => {
        #[inline(always)]
        fn $name(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
            match row.try_get::<$t, usize>(index) {
                Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
                Ok(None) => Ok(py.None()),
                Err(_) => Err(PyValueError::new_err(concat!(
                    "Failed to convert column {} to ",
                    $lbl
                ))),
            }
        }
    };
}

impl_handle_scalar!(handle_int1, u8, "INT1");
impl_handle_scalar!(handle_int2, i16, "INT2");
impl_handle_scalar!(handle_int4, i32, "INT4");
impl_handle_scalar!(handle_int8, i64, "INT8");
impl_handle_scalar!(handle_float4, f32, "FLOAT4");
impl_handle_scalar!(handle_float8, f64, "FLOAT8");

#[inline(always)]
fn handle_nvarchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to NVARCHAR",
            index
        ))),
    }
}

#[inline(always)]
fn handle_varchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to VARCHAR",
            index
        ))),
    }
}

#[inline(always)]
fn handle_bit(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<bool, usize>(index) {
        Ok(Some(val)) => val.into_py_any(py),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to BIT",
            index
        ))),
    }
}

#[inline(always)]
fn handle_binary(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&[u8], usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to BINARY",
            index
        ))),
    }
}

#[inline(always)]
fn handle_money(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => {
            let decimal_class = get_decimal_class(py)?;
            // Avoids floating-point math traps by formatting via string conversion directly
            let s = format!("{:.4}", val);
            Ok(decimal_class.call1((s,))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to MONEY",
            index
        ))),
    }
}

#[inline(always)]
fn handle_money4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => {
            let decimal_class = get_decimal_class(py)?;
            let s = format!("{:.4}", val);
            Ok(decimal_class.call1((s,))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to MONEY4",
            index
        ))),
    }
}

#[inline(always)]
fn handle_decimal(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<tiberius::numeric::Numeric, usize>(index) {
        Ok(Some(numeric)) => {
            let decimal_class = get_decimal_class(py)?;
            Ok(decimal_class.call1((numeric.to_string(),))?.unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DECIMAL",
            index
        ))),
    }
}

#[inline(always)]
fn handle_datetime(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveDateTime, usize>(index) {
        Ok(Some(val)) => val.into_py_any(py),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATETIME",
            index
        ))),
    }
}

#[inline(always)]
fn handle_date(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveDate, usize>(index) {
        Ok(Some(val)) => Ok(val.into_py_any(py)?),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATE",
            index
        ))),
    }
}

#[inline(always)]
fn handle_time(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::NaiveTime, usize>(index) {
        Ok(Some(val)) => Ok(val.into_py_any(py)?),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to TIME",
            index
        ))),
    }
}

#[inline(always)]
fn handle_datetimeoffset(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<chrono::DateTime<chrono::Utc>, usize>(index) {
        Ok(Some(val)) => Ok(val.into_py_any(py)?),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to DATETIMEOFFSET",
            index
        ))),
    }
}

#[inline(always)]
fn handle_uuid(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<uuid::Uuid, usize>(index) {
        Ok(Some(val)) => {
            let mut buf = uuid::Uuid::encode_buffer();
            let uuid_str = val.hyphenated().encode_lower(&mut buf);
            Ok(uuid_str.into_pyobject(py)?.into_any().unbind())
        }
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to UUID",
            index
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
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to XML",
            index
        ))),
    }
}

#[inline(always)]
fn handle_nchar(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to NCHAR",
            index
        ))),
    }
}

/// Handle SQL Server's variable-length nullable integer type (`Intn`).
/// Ordered by demographic likelihood (INT/INT4 and BIGINT/INT8 are statistically primary).
#[inline(always)]
fn handle_intn(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    // 4-byte: INT (Statistically most common database target)
    if let Ok(Some(val)) = row.try_get::<i32, usize>(index) {
        return Ok((val as i64).into_pyobject(py)?.into_any().unbind());
    }
    // 8-byte: BIGINT
    if let Ok(Some(val)) = row.try_get::<i64, usize>(index) {
        return Ok(val.into_pyobject(py)?.into_any().unbind());
    }
    // 2-byte: SMALLINT
    if let Ok(Some(val)) = row.try_get::<i16, usize>(index) {
        return Ok((val as i64).into_pyobject(py)?.into_any().unbind());
    }
    // 1-byte: TINYINT
    if let Ok(Some(val)) = row.try_get::<u8, usize>(index) {
        return Ok((val as i64).into_pyobject(py)?.into_any().unbind());
    }

    // Check for explicit SQL NULL execution across any variant match
    if row.try_get::<i32, usize>(index).map(|v| v.is_none()).unwrap_or(false)
        || row.try_get::<i64, usize>(index).map(|v| v.is_none()).unwrap_or(false)
    {
        return Ok(py.None());
    }

    Err(PyValueError::new_err(format!(
        "Failed to convert column {} to integer",
        index
    )))
}

#[inline(always)]
fn handle_floatn(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    // 8-byte: FLOAT
    if let Ok(Some(val)) = row.try_get::<f64, usize>(index) {
        return Ok(val.into_pyobject(py)?.into_any().unbind());
    }
    // 4-byte: REAL — widen to f64 for Python
    if let Ok(Some(val)) = row.try_get::<f32, usize>(index) {
        return Ok((val as f64).into_pyobject(py)?.into_any().unbind());
    }

    if row.try_get::<f64, usize>(index).map(|v| v.is_none()).unwrap_or(false) {
        return Ok(py.None());
    }

    Err(PyValueError::new_err(format!(
        "Failed to convert column {} to floating-point",
        index
    )))
}

#[inline(always)]
fn handle_fallback(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<&str, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {}",
            index
        ))),
    }
}

pub fn sql_to_python(
    row: &Row,
    index: usize,
    col_type: ColumnType,
    py: Python,
) -> PyResult<Py<PyAny>> {
    match col_type {
        ColumnType::Int4 => handle_int4(row, index, py),
        ColumnType::Int8 => handle_int8(row, index, py),
        ColumnType::Int1 => handle_int1(row, index, py),
        ColumnType::Int2 => handle_int2(row, index, py),
        ColumnType::Intn => handle_intn(row, index, py),
        ColumnType::Float8 => handle_float8(row, index, py),
        ColumnType::Float4 => handle_float4(row, index, py),
        ColumnType::Floatn => handle_floatn(row, index, py),
        ColumnType::NVarchar => handle_nvarchar(row, index, py),
        ColumnType::NChar => handle_nchar(row, index, py),
        ColumnType::BigVarChar | ColumnType::BigChar => handle_varchar(row, index, py),
        ColumnType::Text => handle_varchar(row, index, py),
        ColumnType::NText => handle_nvarchar(row, index, py),
        ColumnType::Image => handle_binary(row, index, py),
        ColumnType::Bit | ColumnType::Bitn => handle_bit(row, index, py),
        ColumnType::Money => handle_money(row, index, py),
        ColumnType::Money4 => handle_money4(row, index, py),
        ColumnType::Decimaln | ColumnType::Numericn => handle_decimal(row, index, py),
        ColumnType::Datetime | ColumnType::Datetimen | ColumnType::Datetime2 => {
            handle_datetime(row, index, py)
        }
        ColumnType::Datetime4 => handle_datetime(row, index, py),
        ColumnType::Daten => handle_date(row, index, py),
        ColumnType::Timen => handle_time(row, index, py),
        ColumnType::DatetimeOffsetn => handle_datetimeoffset(row, index, py),
        ColumnType::Guid => handle_uuid(row, index, py),
        ColumnType::Xml => handle_xml(row, index, py),
        ColumnType::SSVariant => handle_fallback(row, index, py),
        ColumnType::BigVarBin => handle_binary(row, index, py),
        ColumnType::BigBinary => handle_binary(row, index, py),
        ColumnType::Udt => handle_fallback(row, index, py),
        ColumnType::Null => Ok(py.None()),
    }
}

pub fn is_expandable_iterable(obj: &Bound<PyAny>) -> PyResult<bool> {
    // Fast path: scalar types
    if obj.is_instance_of::<PyString>() || obj.is_instance_of::<PyBytes>() {
        return Ok(false);
    }

    // Structural pointer checks (substantially faster than full object casting structures)
    if obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
        || obj.is_instance_of::<PySet>()
        || obj.is_instance_of::<PyFrozenSet>()
    {
        return Ok(true);
    }

    // Dynamic fallback with string lookup tracking optimization
    Ok(obj.hasattr(pyo3::intern!(obj.py(), "__iter__"))?)
}