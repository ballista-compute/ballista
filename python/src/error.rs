use pyo3::exceptions;
use pyo3::prelude::*;
struct BallistaError(ballista_core::error::BallistaError);

impl From<ballista::error::BallistaError> for BallistaError {
    fn from(err: ballista_core::error::BallistaError) -> Self {
        BallistaError(err)
    }
}

impl From<BallistaError> for PyErr {
    fn from(err: BallistaError) -> PyErr {
        exceptions::PyException::new_err(err.0.to_string())
    }
}

impl std::error::Error for BallistaError {}
impl std::fmt::Display for BallistaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ballista_core::error::BallistaError as std::fmt::Display>::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for BallistaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ballista_core::error::BallistaError as std::fmt::Debug>::fmt(&self.0, f)
    }
}

pub fn wrap_kwarg_keytype_error(err: PyErr) -> PyErr {
    pyo3::exceptions::PyTypeError::new_err(format!(
        "kwargs values must be convertible to a string using __str__: {}",
        err.to_string()
    ))
}
