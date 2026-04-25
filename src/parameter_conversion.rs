use crate::py_parameters::Parameters;
use crate::type_mapping;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString};
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub enum FastParameter {
    Null(TypedNull),
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl tiberius::ToSql for FastParameter {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            FastParameter::Null(t) => t.to_sql(),
            FastParameter::Bool(b) => b.to_sql(),
            FastParameter::I64(i) => i.to_sql(),
            FastParameter::F64(f) => f.to_sql(),
            FastParameter::String(s) => s.to_sql(),
            FastParameter::Bytes(b) => b.to_sql(),
        }
    }
}

pub fn python_to_fast_parameter(obj: &Bound<PyAny>) -> PyResult<FastParameter> {
    if obj.is_none() {
        return Ok(FastParameter::Null(TypedNull::U8));
    }

    // Typed nulls
    if let Ok(tn) = obj.extract::<TypedNull>() {
        return Ok(FastParameter::Null(tn))
    }

    if let Ok(py_i) = obj.cast::<PyInt>() {
        return py_i
            .extract::<i64>()
            .map(FastParameter::I64)
            .map_err(|_| PyValueError::new_err("Int too large"));
    }
    if let Ok(py_s) = obj.cast::<PyString>() {
        return Ok(FastParameter::String(py_s.to_str()?.to_owned()));
    }
    if let Ok(py_f) = obj.cast::<PyFloat>() {
        return Ok(FastParameter::F64(py_f.value()));
    }
    if let Ok(py_b) = obj.cast::<PyBool>() {
        return Ok(FastParameter::Bool(py_b.is_true()));
    }
    if let Ok(py_by) = obj.cast::<PyBytes>() {
        return Ok(FastParameter::Bytes(py_by.as_bytes().to_vec()));
    }

    // Fallback for custom types
    if let Ok(i) = obj.extract::<i64>() {
        Ok(FastParameter::I64(i))
    } else {
        Err(PyValueError::new_err(format!(
            "Unsupported type: {}",
            obj.get_type().name()?
        )))
    }
}

pub fn convert_parameters_to_fast(
    parameters: Option<&Bound<PyAny>>,
    py: Python,
) -> PyResult<SmallVec<[FastParameter; 16]>> {
    if let Some(params) = parameters {
        if let Ok(params_obj) = params.extract::<Py<Parameters>>() {
            let list = params_obj.bind(py).call_method0("to_list")?;
            python_params_to_fast_parameters(list.cast::<PyList>()?)
        } else if let Ok(list) = params.cast::<PyList>() {
            python_params_to_fast_parameters(list)
        } else {
            Err(PyValueError::new_err("Must be list or Parameters object"))
        }
    } else {
        Ok(SmallVec::new())
    }
}

fn python_params_to_fast_parameters(
    params: &Bound<PyList>,
) -> PyResult<SmallVec<[FastParameter; 16]>> {
    let len = params.len();

    // SQL Server has a hard limit of 2,100 parameters per query
    if len > 2100 {
        return Err(PyValueError::new_err(format!(
            "Too many parameters: {} provided, but SQL Server supports maximum 2,100 parameters",
            len
        )));
    }

    // SmallVec optimization:
    // - 0-16 parameters: Zero heap allocations (stack only)
    // - 17+ parameters: Single heap allocation (very rare case)
    // - No unnecessary into_vec() conversion
    let mut result: SmallVec<[FastParameter; 16]> = SmallVec::with_capacity(len);

    for param in params.iter() {
        if type_mapping::is_expandable_iterable(&param)? {
            let approx_size = get_iterable_size(&param)?;
            if result.len() + approx_size > 2100 {
                return Err(PyValueError::new_err(format!(
                    "Parameter expansion would exceed SQL Server limit of 2,100 parameters: current {} + expansion {} > 2,100",
                    result.len(),
                    approx_size
                )));
            }

            expand_iterable_to_fast_params(&param, &mut result)?;

            if result.len() > 2100 {
                return Err(PyValueError::new_err(format!(
                    "Parameter expansion exceeded SQL Server limit of 2,100 parameters: {} parameters after expansion",
                    result.len()
                )));
            }
        } else {
            result.push(python_to_fast_parameter(&param)?);
            if result.len() > 2100 {
                return Err(PyValueError::new_err(format!(
                    "Parameter limit exceeded: {} parameters, but SQL Server supports maximum 2,100 parameters",
                    result.len()
                )));
            }
        }
    }

    Ok(result)
}

fn get_iterable_size(iterable: &Bound<PyAny>) -> PyResult<usize> {
    use pyo3::types::{PyList, PyTuple};

    if let Ok(list) = iterable.cast::<PyList>() {
        return Ok(list.len());
    }

    if let Ok(tuple) = iterable.cast::<PyTuple>() {
        return Ok(tuple.len());
    }

    match iterable.call_method0("__len__") {
        Ok(len_result) => {
            if let Ok(size) = len_result.extract::<usize>() {
                return Ok(size);
            }
        }
        Err(_) => {}
    }

    Ok(2101)
}

/// Expand a Python iterable into individual FastParameter objects with minimal allocations
fn expand_iterable_to_fast_params<T>(iterable: &Bound<PyAny>, result: &mut T) -> PyResult<()>
where
    T: Extend<FastParameter>,
{
    use pyo3::types::{PyList, PyTuple};

    // Fast path for common collection types - avoid iterator overhead
    if let Ok(list) = iterable.cast::<PyList>() {
        for item in list.iter() {
            let param = python_to_fast_parameter(&item)?;
            result.extend(std::iter::once(param));
        }
        return Ok(());
    }

    if let Ok(tuple) = iterable.cast::<PyTuple>() {
        for item in tuple.iter() {
            let param = python_to_fast_parameter(&item)?;
            result.extend(std::iter::once(param));
        }
        return Ok(());
    }

    // Fallback for generic iterables - use PyO3's optimized iteration
    let py = iterable.py();
    let iter = iterable.call_method0("__iter__")?;

    let mut batch: SmallVec<[FastParameter; 16]> = SmallVec::new();

    loop {
        match iter.call_method0("__next__") {
            Ok(item) => {
                batch.push(python_to_fast_parameter(&item)?);

                // Batch extend every 16 items to reduce extend() call overhead
                if batch.len() == 16 {
                    result.extend(batch.drain(..));
                }
            }
            Err(err) => {
                // Check if it's StopIteration (normal end of iteration)
                if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                    break;
                } else {
                    return Err(err);
                }
            }
        }
    }

    // Extend any remaining items in the batch
    if !batch.is_empty() {
        result.extend(batch);
    }

    Ok(())
}

/// Class to store a typed null value
/// 
/// This is required as some SQL Server features such as stored procedures etc. sometimes require type information for which is 
/// not possible for nulls when just using `None`. In such cases, SQL Server will complain about being unable to cast 'tinyint'
/// to the desired data type.
#[pyclass(name = "TypedNull", from_py_object)]
#[derive(Debug, Clone)]
pub enum TypedNull {
    U8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bit,
    String,
    Guid,
    Binary,
    Numeric,
    Xml,
    DateTime,
    SmallDateTime,
    Time,
    Date,
    DateTime2,
    DateTimeOffset
}

impl tiberius::ToSql for TypedNull {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            TypedNull::U8 => tiberius::ColumnData::U8(None),
            TypedNull::I16 => tiberius::ColumnData::I16(None),
            TypedNull::I32 => tiberius::ColumnData::I32(None),
            TypedNull::I64 => tiberius::ColumnData::I64(None),
            TypedNull::F32 => tiberius::ColumnData::F32(None),
            TypedNull::F64 => tiberius::ColumnData::F64(None),
            TypedNull::Bit => tiberius::ColumnData::Bit(None),
            TypedNull::String => tiberius::ColumnData::String(None),
            TypedNull::Guid => tiberius::ColumnData::Guid(None),
            TypedNull::Binary => tiberius::ColumnData::Binary(None),
            TypedNull::Numeric => tiberius::ColumnData::Numeric(None),
            TypedNull::Xml => tiberius::ColumnData::Xml(None),
            TypedNull::DateTime => tiberius::ColumnData::DateTime(None),
            TypedNull::SmallDateTime => tiberius::ColumnData::SmallDateTime(None),
            TypedNull::Time => tiberius::ColumnData::Time(None),
            TypedNull::Date => tiberius::ColumnData::Date(None),
            TypedNull::DateTime2 => tiberius::ColumnData::DateTime2(None),
            TypedNull::DateTimeOffset => tiberius::ColumnData::DateTimeOffset(None),
        }
    }
}

#[pymethods]
impl TypedNull {
    #[classattr]
    const TINYINT: TypedNull = TypedNull::U8;
    #[classattr]
    const SMALLINT: TypedNull = TypedNull::I16;
    #[classattr]
    const INT: TypedNull = TypedNull::I32;
    #[classattr]
    const BIGINT: TypedNull = TypedNull::I64;
    #[classattr]
    const FLOAT32: TypedNull = TypedNull::F32;
    #[classattr]
    const FLOAT64: TypedNull = TypedNull::F64;
    #[classattr]
    const BIT: TypedNull = TypedNull::Bit;
    #[classattr]
    const STRING: TypedNull = TypedNull::String;
    #[classattr]
    const GUID: TypedNull = TypedNull::Guid;
    #[classattr]
    const BINARY: TypedNull = TypedNull::Binary;
    #[classattr]
    const NUMERIC: TypedNull = TypedNull::Numeric;
    #[classattr]
    const XML: TypedNull = TypedNull::Xml;
    #[classattr]
    const DATETIME: TypedNull = TypedNull::DateTime;
    #[classattr]
    const SMALLDATETIME: TypedNull = TypedNull::SmallDateTime;
    #[classattr]
    const TIME: TypedNull = TypedNull::Time;
    #[classattr]
    const DATE: TypedNull = TypedNull::Date;
    #[classattr]
    const DATETIME2: TypedNull = TypedNull::DateTime2;
    #[classattr]
    const DATETIMEOFFSET: TypedNull = TypedNull::DateTimeOffset;

    pub fn __str__(&self) -> String {
        match self {
            TypedNull::U8 => "TINYINT".into(),
            TypedNull::I16 => "SMALLINT".into(),
            TypedNull::I32 => "INT".into(),
            TypedNull::I64 => "BIGINT".into(),
            TypedNull::F32 => "FLOAT32".into(),
            TypedNull::F64 => "FLOAT64".into(),
            TypedNull::Bit => "BIT".into(),
            TypedNull::String => "STRING".into(),
            TypedNull::Guid => "GUID".into(),
            TypedNull::Binary => "BINARY".into(),
            TypedNull::Numeric => "NUMERIC".into(),
            TypedNull::Xml => "XML".into(),
            TypedNull::DateTime => "DATETIME".into(),
            TypedNull::SmallDateTime => "SMALLDATETIME".into(),
            TypedNull::Time => "TIME".into(),
            TypedNull::Date => "DATE".into(),
            TypedNull::DateTime2 => "DATETIME2".into(),
            TypedNull::DateTimeOffset => "DATETIMEOFFSET".into(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("TypedNull.{}", self.__str__())
    }
}