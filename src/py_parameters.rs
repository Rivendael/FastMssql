use pyo3::prelude::*;
use pyo3::types::{PyList, PyDict, PyTuple};
use crate::type_mapping;

#[pyclass]
pub struct Parameter {
    #[pyo3(get)]
    pub value: Py<PyAny>,
    #[pyo3(get)]
    pub sql_type: Option<String>,
    #[pyo3(get)]
    pub is_expanded: bool,
}

#[pymethods]
impl Parameter {
    #[new]
    #[pyo3(signature = (value, sql_type=None))]
    pub fn new(value: Py<PyAny>, sql_type: Option<String>) -> Self {
        let is_expanded = Python::attach(|py| {
            let value_bound = value.bind(py);
            type_mapping::is_expandable_iterable(&value_bound).unwrap_or(false)
        });
        
        Parameter {
            value,
            sql_type,
            is_expanded,
        }
    }

    fn __repr__(&self, py: Python) -> String {
        let value_bound = self.value.bind(py);
        let value_repr = match value_bound.repr() {
            Ok(repr) => repr.to_string(),
            Err(_) => "<error>".to_string(),
        };
        
        // Check if this is an expanded parameter (iterable)
        if self.is_expanded {
            match &self.sql_type {
                Some(sql_type) => format!("Parameter(IN_values={}, type={})", value_repr, sql_type),
                None => format!("Parameter(IN_values={})", value_repr),
            }
        } else {
            match &self.sql_type {
                Some(sql_type) => format!("Parameter(value={}, type={})", value_repr, sql_type),
                None => format!("Parameter(value={})", value_repr),
            }
        }
    }
}

impl Parameter {}

#[pyclass]
pub struct Parameters {
    #[pyo3(get)]
    pub positional: Vec<Py<Parameter>>,
    #[pyo3(get)]
    pub named: Py<PyDict>,
}

#[pymethods]
impl Parameters {
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(py: Python, args: &Bound<PyTuple>, kwargs: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let mut positional = Vec::new();
        let named = PyDict::new(py);

        // Add positional arguments
        for arg in args.iter() {
            // Check if the argument is already a Parameter object
            if let Ok(existing_param) = arg.extract::<Py<Parameter>>() {
                // Use the existing Parameter object directly
                positional.push(existing_param);
            } else {
                // Create a new Parameter for raw values
                let param = Parameter::new(arg.unbind(), None);
                positional.push(Py::new(py, param)?);
            }
        }

        // Add named arguments
        if let Some(kwargs_dict) = kwargs {
            for (key, value) in kwargs_dict.iter() {
                // Check if the value is already a Parameter object
                if let Ok(existing_param) = value.extract::<Py<Parameter>>() {
                    named.set_item(key, existing_param)?;
                } else {
                    // Create a new Parameter for raw values
                    let param = Parameter::new(value.unbind(), None);
                    named.set_item(key, Py::new(py, param)?)?;
                }
            }
        }

        Ok(Parameters { 
            positional, 
            named: named.into() 
        })
    }

    #[pyo3(signature = (value, sql_type=None))]
    pub fn add(mut slf: PyRefMut<Self>, py: Python, value: Py<PyAny>, sql_type: Option<String>) -> PyResult<Py<Parameters>> {
        let param = Parameter::new(value, sql_type);
        slf.positional.push(Py::new(py, param)?);
        
        // Return the same Python object for chaining
        Ok(slf.into())
    }

    #[pyo3(signature = (key, value, sql_type=None))]
    pub fn set(slf: PyRefMut<Self>, py: Python, key: String, value: Py<PyAny>, sql_type: Option<String>) -> PyResult<Py<Parameters>> {
        let param = Parameter::new(value, sql_type);
        
        // Add the new parameter to existing named dict
        slf.named.bind(py).set_item(key, Py::new(py, param)?)?;
        
        // Return the same Python object for chaining
        Ok(slf.into())
    }

    pub fn to_list(&self, py: Python) -> PyResult<Py<PyList>> {
        let mut values = Vec::new();
        for param_py in &self.positional {
            let param = param_py.borrow(py);
            values.push(param.value.clone_ref(py));
        }
        Ok(PyList::new(py, values)?.into())
    }

    fn __len__(&self, py: Python) -> usize {
        let named_len = self.named.bind(py).len();
        self.positional.len() + named_len
    }

    fn __repr__(&self, py: Python) -> String {
        let positional_len = self.positional.len();
        let named_len = self.named.bind(py).len();
        
        if positional_len == 0 && named_len == 0 {
            "Parameters()".to_string()
        } else if positional_len > 0 && named_len == 0 {
            format!("Parameters(positional={})", positional_len)
        } else if positional_len == 0 && named_len > 0 {
            format!("Parameters(named={})", named_len)
        } else {
            format!("Parameters(positional={}, named={})", positional_len, named_len)
        }
    }

    #[getter]
    fn positional(&self, py: Python) -> Vec<Py<Parameter>> {
        // Return a copy of the positional parameters
        self.positional.iter().map(|p| p.clone_ref(py)).collect()
    }

    #[getter]
    fn named(&self, py: Python) -> PyResult<Py<PyDict>> {
        // Return a copy of the named parameters dictionary
        let new_dict = PyDict::new(py);
        let named_dict = self.named.bind(py);
        for (key, value) in named_dict.iter() {
            new_dict.set_item(key, value)?;
        }
        Ok(new_dict.into())
    }
}
