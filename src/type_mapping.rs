use std::fmt::Write;
use std::sync::OnceLock;

use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyFrozenSet, PyList, PySet, PyString, PyTuple};
use pyo3::{IntoPyObjectExt, Py, PyAny, prelude::*};
use tiberius::{ColumnType, Row};

/// Cached handle to `decimal.Decimal` — imported once, reused for every row.
///
/// `Py<PyAny>` is `Send + Sync`, so storing it in a `static` is safe.
/// On every call the raw pointer is re-bound to the current `Python<'_>` token
/// via `.bind(py)`, which is a zero-cost type-level rebrand (no refcount change).
static DECIMAL_CLASS: OnceLock<Py<PyAny>> = OnceLock::new();

/// Return a `Bound` reference to `decimal.Decimal`, initialising the cache on
/// the very first call and simply re-binding on every subsequent call.
///
/// `OnceLock::get_or_try_init` is nightly-only (`once_cell_try`, issue #109737).
/// The stable equivalent is: check `get()`, lazily initialise, then `set()`.
/// If two threads race to call this simultaneously the `set()` that loses is
/// silently discarded — `get()` after the race is always `Some`.
#[inline]
fn get_decimal_class(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    // Fast path: already initialised.
    if let Some(cls) = DECIMAL_CLASS.get() {
        return Ok(cls.bind(py).clone());
    }
    // Slow path: import and cache.
    let cls = py.import("decimal")?.getattr("Decimal")?.unbind();
    // Ignore Err — a concurrent thread may have won the race; either way
    // `get()` below is guaranteed to return `Some`.
    let _ = DECIMAL_CLASS.set(cls);
    Ok(DECIMAL_CLASS.get().unwrap().bind(py).clone())
}

#[inline(always)]
fn handle_int4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT4",
            index
        ))),
    }
}

#[inline(always)]
fn handle_int8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT8",
            index
        ))),
    }
}

#[inline(always)]
fn handle_int1(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<u8, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT1",
            index
        ))),
    }
}

#[inline(always)]
fn handle_int2(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<i16, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to INT2",
            index
        ))),
    }
}

#[inline(always)]
fn handle_float8(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to FLOAT8",
            index
        ))),
    }
}

#[inline(always)]
fn handle_float4(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f32, usize>(index) {
        Ok(Some(val)) => Ok((val as f64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to FLOAT4",
            index
        ))),
    }
}

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

/// Format a MONEY wire value (integer * 10^-4) into a decimal string.
///
/// Uses `String::with_capacity(24)` so the buffer is sized to the maximum
/// representable MONEY width up-front, avoiding any reallocation inside
/// `write!`.  The `write!` call into `String` is infallible, so `.unwrap()`
/// can never panic.
#[inline(always)]
fn money_to_decimal_string(val: f64) -> String {
    let units = (val * 10_000.0).round() as i64;
    let sign = if units < 0 { "-" } else { "" };
    let abs_units = units.unsigned_abs();
    let mut s = String::with_capacity(24);
    write!(
        s,
        "{}{}.{:04}",
        sign,
        abs_units / 10_000,
        abs_units % 10_000
    )
    .unwrap();
    s
}

#[inline(always)]
fn handle_money(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => {
            let decimal_class = get_decimal_class(py)?;
            Ok(decimal_class
                .call1((money_to_decimal_string(val),))?
                .unbind())
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
            Ok(decimal_class
                .call1((money_to_decimal_string(val),))?
                .unbind())
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

/// Convert a UUID column to a Python string using a stack-allocated 45-byte
/// buffer.  `uuid::Uuid::encode_buffer()` returns `[u8; 45]` on the stack;
/// `.hyphenated().encode_lower(&mut buf)` writes the standard 8-4-4-4-12 form
/// directly into it and returns a `&str` — zero heap allocations.
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
///
/// Tiberius maps TINYINT / SMALLINT / INT / BIGINT to this single enum variant
/// when the column is nullable.  The underlying wire representation is 1, 2, 4,
/// or 8 bytes respectively.  Attempting `try_get::<i64>` on a 2-byte SMALLINT
/// will return an error, so we cascade from the largest type to the smallest and
/// return on the first success or the first `Ok(None)` (SQL NULL).
#[inline(always)]
fn handle_intn(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    // 8-byte: BIGINT
    match row.try_get::<i64, usize>(index) {
        Ok(Some(val)) => return Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => return Ok(py.None()),
        Err(_) => {}
    }
    // 4-byte: INT
    match row.try_get::<i32, usize>(index) {
        Ok(Some(val)) => return Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => return Ok(py.None()),
        Err(_) => {}
    }
    // 2-byte: SMALLINT
    match row.try_get::<i16, usize>(index) {
        Ok(Some(val)) => return Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => return Ok(py.None()),
        Err(_) => {}
    }
    // 1-byte: TINYINT
    match row.try_get::<u8, usize>(index) {
        Ok(Some(val)) => Ok((val as i64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to integer",
            index
        ))),
    }
}

/// Handle SQL Server's variable-length nullable float type (`Floatn`).
///
/// Tiberius maps REAL (4-byte) and FLOAT (8-byte) to this single variant when
/// nullable.  We try `f64` first (FLOAT / 8-byte) and fall back to `f32`
/// (REAL / 4-byte), widening it to `f64` for Python.
#[inline(always)]
fn handle_floatn(row: &Row, index: usize, py: Python) -> PyResult<Py<PyAny>> {
    // 8-byte: FLOAT
    match row.try_get::<f64, usize>(index) {
        Ok(Some(val)) => return Ok(val.into_pyobject(py)?.into_any().unbind()),
        Ok(None) => return Ok(py.None()),
        Err(_) => {}
    }
    // 4-byte: REAL — widen to f64 for Python
    match row.try_get::<f32, usize>(index) {
        Ok(Some(val)) => Ok((val as f64).into_pyobject(py)?.into_any().unbind()),
        Ok(None) => Ok(py.None()),
        Err(_) => Err(PyValueError::new_err(format!(
            "Failed to convert column {} to floating-point",
            index
        ))),
    }
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
        ColumnType::Intn => handle_intn(row, index, py), // Variable-length integer
        ColumnType::Float8 => handle_float8(row, index, py),
        ColumnType::Float4 => handle_float4(row, index, py),
        ColumnType::Floatn => handle_floatn(row, index, py), // Variable-length float
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

/// Check if a Python object is an iterable that should be expanded for parameters.
///
/// Returns `true` for lists, tuples, sets, etc., but `false` for strings and
/// bytes which must be treated as single scalar values.
///
/// # Fast paths (in order)
/// 1. [`PyString`] / [`PyBytes`] → always `false` (no attribute lookup).
/// 2. [`PyList`] / [`PyTuple`] / [`PySet`] / [`PyFrozenSet`] → always `true`
///    (direct type-pointer comparison via `cast`).
/// 3. Custom classes → `obj.is_iterable()`, which calls `ffi::PyIter_Check`
///    (a single C pointer dereference — no GIL attribute lookup).
pub fn is_expandable_iterable(obj: &Bound<PyAny>) -> PyResult<bool> {
    // Fast path: scalar string/byte types are never expanded.
    if obj.is_instance_of::<PyString>() || obj.is_instance_of::<PyBytes>() {
        return Ok(false);
    }

    // Direct structural checks for the four built-in collection types.
    if obj.cast::<PyList>().is_ok()
        || obj.cast::<PyTuple>().is_ok()
        || obj.cast::<PySet>().is_ok()
        || obj.cast::<PyFrozenSet>().is_ok()
    {
        return Ok(true);
    }

    // Custom iterable fallback: check for __iter__ via PyO3's safe hasattr.
    //
    // PyO3 0.28 does not expose a safe `is_iterable()` wrapper on Bound<PyAny>,
    // and the underlying `tp_iter` slot is inaccessible under the abi3 limited
    // API.  `hasattr("__iter__")` is the correct safe idiomatic approach here —
    // PyO3 interns the attribute name string, so this is a single GIL-held
    // pointer comparison in the common (cached) case.
    Ok(obj.hasattr("__iter__")?)
}
