use pyo3::prelude::PyErr;
use pyo3::{
    types::{PyDict, PyTuple},
    FromPyObject,
};

use pyo3::exceptions::PyException;
use pyo3::prelude::PyResult;
use std::collections::HashMap;

pub fn wrap_err<E: std::error::Error>(err: E) -> PyErr {
    PyException::new_err(err.to_string())
}

pub fn kwargs_to_string_map(kwargs: Option<&PyDict>) -> PyResult<HashMap<String, String>> {
    Ok(match kwargs {
        Some(kwargs) => kwargs
            .iter()
            .map(|(py_key_obj, py_val_obj)| {
                let py_key = py_key_obj.str().map_err(|err| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "kwargs keys must be convertible to a string using str(): {}",
                        err.to_string()
                    ))
                })?;
                let py_val = py_val_obj.str().map_err(|err| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "kwargs values must be convertible to a string using str(): {}",
                        err.to_string()
                    ))
                })?;
                Ok((py_key.to_str()?.to_owned(), py_val.to_str()?.to_owned()))
            })
            .collect::<Result<HashMap<String, String>, PyErr>>()?,
        None => HashMap::new(),
    })
}

pub fn missing_key(key_name: &str, from_name: &str) -> PyErr {
    PyException::new_err(format!(
        "The required key {} was not present in {}.",
        key_name, from_name
    ))
}

pub fn transform_tuple_to_uniform_type<'a, T: FromPyObject<'a>, U, F: Fn(T) -> U>(
    tuple: &'a PyTuple,
    transform: F,
) -> PyResult<Vec<U>> {
    tuple
        .iter()
        .map(|tuple_item| Ok(transform(tuple_item.extract::<T>()?)))
        .collect::<Result<Vec<U>, PyErr>>()
}

pub fn tuple_to_uniform_type<'a, T: FromPyObject<'a>>(tuple: &'a PyTuple) -> PyResult<Vec<T>> {
    tuple
        .iter()
        .map(|tuple_item| tuple_item.extract::<T>())
        .collect::<Result<Vec<T>, PyErr>>()
}
use pyo3::PyAny;
pub fn object_class_name(ob: &PyAny) -> PyResult<String> {
    let qual_name = ob
        .getattr("__class__")?
        .getattr("__qualname__")?
        .extract::<&str>()?;
    let module_name = ob
        .getattr("__class__")?
        .getattr("__module__")?
        .extract::<&str>()?;
    Ok(format!("{}.{}", module_name, qual_name))
}
