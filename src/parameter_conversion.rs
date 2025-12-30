use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString};
use pyo3::exceptions::PyValueError;
use smallvec::SmallVec;
use crate::py_parameters::Parameters;
use crate::type_mapping;

#[derive(Debug, Clone)]
pub enum FastParameter {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl tiberius::ToSql for FastParameter {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            FastParameter::Null => tiberius::ColumnData::U8(None),
            FastParameter::Bool(b) => b.to_sql(),
            FastParameter::I64(i) => i.to_sql(),
            FastParameter::F64(f) => f.to_sql(),
            FastParameter::String(s) => s.to_sql(),
            FastParameter::Bytes(b) => b.to_sql(),
        }
    }
}

pub fn python_to_fast_parameter(obj: &Bound<PyAny>) -> PyResult<FastParameter> {
    if obj.is_none() { return Ok(FastParameter::Null); }
    if let Ok(py_s) = obj.cast::<PyString>() { return Ok(FastParameter::String(py_s.to_str()?.to_owned())); }
    if let Ok(py_i) = obj.cast::<PyInt>() { return py_i.extract::<i64>().map(FastParameter::I64).map_err(|_| PyValueError::new_err("Int too large")); }
    if let Ok(py_f) = obj.cast::<PyFloat>() { return Ok(FastParameter::F64(py_f.value())); }
    if let Ok(py_b) = obj.cast::<PyBool>() { return Ok(FastParameter::Bool(py_b.is_true())); }
    if let Ok(py_by) = obj.cast::<PyBytes>() { return Ok(FastParameter::Bytes(py_by.as_bytes().to_vec())); }
    
    // Fallback for custom types
    if let Ok(i) = obj.extract::<i64>() { Ok(FastParameter::I64(i)) }
    else { Err(PyValueError::new_err(format!("Unsupported type: {}", obj.get_type().name()?))) }
}

pub fn convert_parameters_to_fast(parameters: Option<&Bound<PyAny>>, py: Python) -> PyResult<SmallVec<[FastParameter; 16]>> {
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

    // SmallVec optimization:
    // - 0-16 parameters: Zero heap allocations (stack only)
    // - 17+ parameters: Single heap allocation (very rare case)
    // - No unnecessary into_vec() conversion
    let mut result: SmallVec<[FastParameter; 16]> = SmallVec::with_capacity(len);

    for param in params.iter() {
        if type_mapping::is_expandable_iterable(&param)? {
            expand_iterable_to_fast_params(&param, &mut result)?;
        } else {
            result.push(python_to_fast_parameter(&param)?);
        }
    }

    Ok(result)
}

/// Expand a Python iterable into individual FastParameter objects with minimal allocations
fn expand_iterable_to_fast_params<T>(iterable: &Bound<PyAny>, result: &mut T) -> PyResult<()>
where
    T: Extend<FastParameter>,
{
    use pyo3::types::{PyList, PyTuple};

    // Fast path for common collection types - avoid iterator overhead and intermediate Vec allocation
    if let Ok(list) = iterable.cast::<PyList>() {
        for item in list.iter() {
            result.extend(std::iter::once(python_to_fast_parameter(&item)?));
        }
        return Ok(());
    }

    if let Ok(tuple) = iterable.cast::<PyTuple>() {
        for item in tuple.iter() {
            result.extend(std::iter::once(python_to_fast_parameter(&item)?));
        }
        return Ok(());
    }

    // Fallback for generic iterables - use PyO3's optimized iteration
    let py = iterable.py();
    let iter = iterable.call_method0("__iter__")?;

    // Pre-allocate a small buffer to batch extend operations and reduce allocations
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