//Allow non snake case names so that we can pretend the function Union is a class in python, this allows a more uniform interface with the types
#![allow(non_snake_case)]

use crate::field::BPyField;
use crate::util;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::{IntervalUnit, TimeUnit};
use pyo3::types::PyTuple;
use pyo3::{exceptions::PyException, prelude::*};

#[pyclass(name = "DataType", module = "ballista")]
#[derive(Clone)]
#[repr(transparent)]
pub struct BPyDataType {
    pub(crate) datatype: DataType,
}

#[pymethods]
impl BPyDataType {
    #[new]
    fn new(primitive: &PyAny) -> PyResult<BPyDataType> {
        let datatype = any_to_datatype(primitive)?;
        Ok(BPyDataType { datatype })
    }

    fn is_numeric(&self) -> bool {
        DataType::is_numeric(&self.datatype)
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let datatype: DataType = serde_json::from_str(json).map_err(util::wrap_err)?;
        Ok(BPyDataType { datatype })
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.datatype).map_err(util::wrap_err)
    }
}

use pyo3::basic::PyObjectProtocol;
#[pyproto]
impl PyObjectProtocol for BPyDataType {
    fn __str__(&self) -> String {
        format!("{}", self.datatype)
    }
}

#[pyfunction]
fn List(item_type: BPyField) -> BPyDataType {
    BPyDataType {
        datatype: DataType::List(Box::new(item_type.arrow_field)),
    }
}

#[pyfunction]
fn LargeList(item_type: BPyField) -> BPyDataType {
    BPyDataType {
        datatype: DataType::List(Box::new(item_type.arrow_field)),
    }
}

#[pyfunction]
fn FixedSizeList(item_type: BPyField, list_size: i32) -> PyResult<BPyDataType> {
    if list_size < 0 {
        return Err(PyException::new_err(format!(
            "list_size must be postive, found :{}",
            list_size
        )));
    }
    Ok(BPyDataType {
        datatype: DataType::FixedSizeList(Box::new(item_type.arrow_field), list_size),
    })
}

#[pyfunction]
fn FixedSizeBinary(binary_size: i32) -> PyResult<BPyDataType> {
    if binary_size < 0 {
        return Err(PyException::new_err(format!(
            "binary_size must be postive, found :{}",
            binary_size
        )));
    }
    Ok(BPyDataType {
        datatype: DataType::FixedSizeBinary(binary_size),
    })
}

#[pyfunction(interval_unit = "None")]
fn Interval(interval_unit: Option<&PyAny>) -> PyResult<BPyDataType> {
    let native_interval_unit = match interval_unit {
        Some(any) => any_to_interval_unit(any)?,
        None => IntervalUnit::DayTime,
    };
    Ok(BPyDataType {
        datatype: DataType::Interval(native_interval_unit),
    })
}

#[pyfunction(date_unit = "None", date32 = "false")]
fn Date(date32: bool) -> PyResult<BPyDataType> {
    Ok(BPyDataType {
        datatype: match date32 {
            true => DataType::Date32,
            false => DataType::Date64,
        },
    })
}

#[pyfunction(time_unit = "None", time32 = "false")]
#[text_signature = "(time_unit:typing.Optional[typing.Union['TimeUnit', str]], time32: bool = False)"]
fn Time(time_unit: Option<&PyAny>, time32: bool) -> PyResult<BPyDataType> {
    let native_time_unit = match time_unit {
        Some(any) => any_to_time_unit(any)?,
        None => TimeUnit::Millisecond,
    };
    Ok(BPyDataType {
        datatype: match time32 {
            true => DataType::Time32(native_time_unit),
            false => DataType::Time64(native_time_unit),
        },
    })
}

#[pyfunction(time_unit = "None")]
fn Duration(time_unit: Option<&PyAny>) -> PyResult<BPyDataType> {
    let native_time_unit = match time_unit {
        Some(any) => any_to_time_unit(any)?,
        None => TimeUnit::Millisecond,
    };
    Ok(BPyDataType {
        datatype: DataType::Duration(native_time_unit),
    })
}

#[pyfunction(fields = "*")]
fn Union(fields: &PyTuple) -> PyResult<BPyDataType> {
    if fields.is_empty() {
        return Err(PyException::new_err(
            "To construct a Union at least a single Field must be provided",
        ));
    }
    let union_fields = fields
        .iter()
        .map(|any| Ok(any.extract::<BPyField>()?.arrow_field))
        .collect::<PyResult<Vec<Field>>>()?;
    Ok(BPyDataType {
        datatype: DataType::Union(union_fields),
    })
}

#[pyfunction(members = "*")]
fn Struct(members: &PyTuple) -> PyResult<BPyDataType> {
    if members.is_empty() {
        return Err(PyException::new_err(
            "To construct a Struct at least a single Field must be provided",
        ));
    }
    let struct_members = members
        .iter()
        .map(|any| Ok(any.extract::<BPyField>()?.arrow_field))
        .collect::<PyResult<Vec<Field>>>()?;
    Ok(BPyDataType {
        datatype: DataType::Struct(struct_members),
    })
}

#[pyfunction]
fn Dictionary(key_type: BPyDataType, value_type: BPyDataType) -> BPyDataType {
    BPyDataType {
        datatype: DataType::Dictionary(Box::new(key_type.datatype), Box::new(value_type.datatype)),
    }
}

#[pyfunction]
fn Decimal(whole: usize, fractional: usize) -> BPyDataType {
    BPyDataType {
        datatype: DataType::Decimal(whole, fractional),
    }
}

#[pyclass(name = "TimeUnit", module = "ballista")]
#[derive(Clone)]
#[repr(transparent)]
pub struct BPyTimeUnit {
    unit: TimeUnit,
}

fn any_to_time_unit(any: &PyAny) -> PyResult<TimeUnit> {
    if let Ok(native_time_unit) = any.extract::<BPyTimeUnit>() {
        Ok(native_time_unit.unit)
    } else if let Ok(time_unit_str) = any.extract::<&str>() {
        Ok(match time_unit_str {
            "Second" | "second" | "s" => TimeUnit::Second,
            "Millisecond" | "MilliSecond" | "ms" | "milli" | "Milli" => TimeUnit::Millisecond,
            "Microsecond" | "MicroSecond" | "us" | "micro" | "Micro" => TimeUnit::Microsecond,
            "Nanosecond" | "NanoSecond" | "ns" | "nano" | "Nano" => TimeUnit::Nanosecond,
            _ => {
                return Err(PyException::new_err(format!(
                    "Could not convert '{}' to a valid time unit",
                    time_unit_str
                )))
            }
        })
    } else {
        let type_name = util::object_class_name(any);
        Err(PyException::new_err(format!(
            "Could not convert {} to a valid time unit",
            match type_name.as_ref() {
                Ok(name) => name.as_str(),
                Err(_) => "<Could not get class name>",
            }
        )))
    }
}

#[pyclass(name = "IntervalUnit", module = "ballista")]
#[derive(Clone)]
#[repr(transparent)]
pub struct BPyIntervalUnit {
    unit: IntervalUnit,
}

fn any_to_interval_unit(any: &PyAny) -> PyResult<IntervalUnit> {
    if let Ok(native_interval_unit) = any.extract::<BPyIntervalUnit>() {
        Ok(native_interval_unit.unit)
    } else if let Ok(interval_unit_str) = any.extract::<&str>() {
        Ok(match interval_unit_str {
            "YearMonth" | "ym" | "y" => IntervalUnit::YearMonth,
            "DayTime" | "dt" | "d" => IntervalUnit::DayTime,
            _ => {
                return Err(PyException::new_err(format!(
                    "Could not convert '{}' to a valid interval unit",
                    interval_unit_str
                )))
            }
        })
    } else {
        let type_name = util::object_class_name(any);
        Err(PyException::new_err(format!(
            "Could not convert {} to a valid interval unit",
            match type_name.as_ref() {
                Ok(name) => name.as_str(),
                Err(_) => "<Could not get class name>",
            }
        )))
    }
}

pub fn any_to_datatype(any: &PyAny) -> PyResult<DataType> {
    if let Ok(datatype) = any.extract::<BPyDataType>() {
        Ok(datatype.datatype)
    } else if let Ok(datatype_str) = any.extract::<&str>() {
        Ok(match datatype_str {
            "int8" | "i8" => DataType::Int8,
            "int16" | "i16" => DataType::Int16,
            "int32" | "int" | "i32" => DataType::Int32,
            "int64" | "i64" => DataType::Int64,
            "uint8" | "u8" => DataType::UInt8,
            "uint16" | "u16" => DataType::UInt16,
            "uint32" | "uint" | "u32" => DataType::UInt32,
            "uint64" | "u64" => DataType::UInt64,
            "str" | "utf8" => DataType::Utf8,
            "float32" | "f32" => DataType::Float32,
            "float" | "f64" => DataType::Float64,
            "bool" => DataType::Boolean,
            "date32" => DataType::Date32,
            "date64" => DataType::Date64,
            _ => {
                return Err(PyException::new_err(format!(
                    "The type string {} is invalid",
                    datatype_str
                )))
            }
        })
    } else {
        let py_type = any.get_type();
        let type_name = py_type
            .str()
            .map(|p| p.to_string())
            .unwrap_or_else(|_| String::from("<Failed to get type name>"));

        Err(PyException::new_err(format!(
            "Could not convert type {}",
            type_name
        )))
    }
}

#[pyfunction]
fn Timestamp(time_unit: &PyAny, timezone: Option<&str>) -> PyResult<BPyDataType> {
    let native_tu = any_to_time_unit(time_unit)?;
    Ok(BPyDataType {
        datatype: DataType::Timestamp(
            native_tu,
            match timezone {
                Some(tz) => Some(tz.to_owned()),
                None => None,
            },
        ),
    })
}

pub fn init(m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(Date, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Decimal, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Duration, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Dictionary, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(FixedSizeBinary, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(FixedSizeList, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Interval, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(LargeList, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(List, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Struct, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Time, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Timestamp, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(Union, m)?)?;

    m.add_class::<BPyDataType>()?;
    m.add_class::<BPyTimeUnit>()?;
    m.add_class::<BPyIntervalUnit>()?;

    m.add(
        "Null",
        BPyDataType {
            datatype: DataType::Float32,
        },
    )?;
    m.add(
        "Bool",
        BPyDataType {
            datatype: DataType::Float64,
        },
    )?;
    m.add(
        "Int8",
        BPyDataType {
            datatype: DataType::Int8,
        },
    )?;
    m.add(
        "Int16",
        BPyDataType {
            datatype: DataType::Int16,
        },
    )?;
    m.add(
        "Int32",
        BPyDataType {
            datatype: DataType::Int32,
        },
    )?;
    m.add(
        "Int64",
        BPyDataType {
            datatype: DataType::Int64,
        },
    )?;
    m.add(
        "UInt8",
        BPyDataType {
            datatype: DataType::UInt8,
        },
    )?;
    m.add(
        "UInt16",
        BPyDataType {
            datatype: DataType::UInt16,
        },
    )?;
    m.add(
        "UInt32",
        BPyDataType {
            datatype: DataType::UInt32,
        },
    )?;
    m.add(
        "UInt64",
        BPyDataType {
            datatype: DataType::UInt64,
        },
    )?;
    m.add(
        "Float16",
        BPyDataType {
            datatype: DataType::Float16,
        },
    )?;
    m.add(
        "Float32",
        BPyDataType {
            datatype: DataType::Float32,
        },
    )?;
    m.add(
        "Float64",
        BPyDataType {
            datatype: DataType::Float64,
        },
    )?;
    m.add(
        "Binary",
        BPyDataType {
            datatype: DataType::Binary,
        },
    )?;
    m.add(
        "LargeBinary",
        BPyDataType {
            datatype: DataType::Binary,
        },
    )?;
    m.add(
        "Utf8",
        BPyDataType {
            datatype: DataType::Binary,
        },
    )?;
    m.add(
        "LargeUtf8",
        BPyDataType {
            datatype: DataType::Binary,
        },
    )?;
    Ok(())
}
