use datafusion::scalar::ScalarValue;

use pyo3::types::PyTuple;
use pyo3::{exceptions::PyException, prelude::*};
#[derive(Debug, Clone)]
#[repr(transparent)]
pub(crate) struct Scalar {
    pub(crate) scalar: ScalarValue,
}

impl<'source> FromPyObject<'source> for Scalar {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let qual_name = ob
            .getattr("__class__")?
            .getattr("__qualname__")?
            .extract::<&str>()?;
        let module_name = ob
            .getattr("__class__")?
            .getattr("__module__")?
            .extract::<&str>()?;
        println!("Module qualified type name: {}.{}", module_name, qual_name);

        let scalar_value = match module_name {
            "numpy" => match qual_name {
                "int8" => ScalarValue::Int8(Some(ob.extract::<i8>()?)),
                "int32" => ScalarValue::Int32(Some(ob.extract::<i32>()?)),
                "int16" => ScalarValue::Int16(Some(ob.extract::<i16>()?)),
                "int64" => ScalarValue::Int64(Some(ob.extract::<i64>()?)),
                "uint8" => ScalarValue::UInt8(Some(ob.extract::<u8>()?)),
                "uint16" => ScalarValue::UInt16(Some(ob.extract::<u16>()?)),
                "uint32" => ScalarValue::UInt32(Some(ob.extract::<u32>()?)),
                "uint64" => ScalarValue::UInt64(Some(ob.extract::<u64>()?)),
                "float64" | "float_" => ScalarValue::Float64(Some(ob.extract::<f64>()?)),
                "float32" => ScalarValue::Float32(Some(ob.extract::<f32>()?)),
                "bool_" => ScalarValue::Boolean(Some(ob.extract::<bool>()?)),
                "str_" => ScalarValue::Utf8(Some(ob.extract::<String>()?)),
                "ndarray" => {
                    let np_dtype = ob.getattr("dtype")?;
                    let _datatype_name = np_dtype.getattr("name")?.extract::<&str>()?;
                    let ndarray_shape_py_obj = ob.getattr("shape")?.extract::<&PyTuple>()?;

                    let ndarray_shape = ndarray_shape_py_obj
                        .iter()
                        .map(|tuple_item| tuple_item.extract::<isize>())
                        .collect::<Result<Vec<_>, PyErr>>()?;

                    println!("The shape of the numpy array is {:?}", ndarray_shape);
                    return Err(PyException::new_err("numpy.ndarray is unimplemented"));
                }
                other => {
                    return Err(PyException::new_err(format!(
                        "Numpy type '{}' not yet implemented",
                        other
                    )));
                }
            },
            "builtins" => match qual_name {
                "int" => ScalarValue::Int64(Some(ob.extract::<i64>()?)),
                "float" => ScalarValue::Float64(Some(ob.extract::<f64>()?)),
                "str" => ScalarValue::Utf8(Some(ob.extract::<String>()?)),
                other => {
                    return Err(PyException::new_err(format!(
                        "Builtin type '{}' not yet implemented",
                        other
                    )));
                }
            },
            "ballista" => match qual_name {
                "Scalar" => ob.extract::<Scalar>()?.scalar,
                _ => {
                    return Err(PyException::new_err(format!(
                        "Ballista type '{}' is not supported as a literal scalar value",
                        qual_name
                    )))
                }
            },
            _ => {
                return Err(PyException::new_err(format!(
                    "Types from the module '{}' are not supported. If you would like to have support added please file an issue at https://github.com/ballista-compute/ballista",
                    module_name
                )));
            }
        };

        Ok(Scalar {
            scalar: scalar_value,
        })
    }
}
