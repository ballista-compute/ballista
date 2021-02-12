use crate::field::BPyField;
use crate::util;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
#[pyclass(name = "Schema", module = "ballista")]
#[derive(Clone)]
pub struct BPySchema {
    pub(crate) schema: Schema,
}

#[pymethods]
impl BPySchema {
    #[new]
    #[args(args = "*", kwargs = "**")]
    pub fn new(args: &PyTuple, kwargs: Option<&PyDict>) -> PyResult<Self> {
        let arrow_fields = args
            .iter()
            .map(|arg| match arg.extract::<BPyField>() {
                Ok(field) => Ok(field.arrow_field),
                Err(py_err) => Err(py_err),
            })
            .collect::<Result<Vec<Field>, _>>()?;
        let metadata = util::kwargs_to_string_map(kwargs)?;
        Ok(Self {
            schema: Schema::new_with_metadata(arrow_fields, metadata),
        })
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.schema).map_err(util::wrap_err)
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let val: Schema = serde_json::from_str(json).map_err(util::wrap_err)?;
        Ok(BPySchema { schema: val })
    }
}

use pyo3::basic::PyObjectProtocol;
#[pyproto]
impl PyObjectProtocol for BPySchema {
    fn __str__(&self) -> String {
        let mut msg = String::from("Schema: [\n");
        let mut first = true;
        for field in self.schema.fields().iter() {
            if !first {
                msg.push_str(", \n");
            }
            msg.push_str(&format!(
                "Field{{ name: {}, datatype: {}, nullable: {}}}",
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ));
            first = false;
        }
        msg.push_str("\n]");
        msg
    }
}

impl BPySchema {
    pub fn from_arrow_schema(schema: Schema) -> Self {
        Self { schema }
    }
}

use datafusion::logical_plan::DFSchemaRef;
#[pyclass(name = "DFSchema", module = "ballista")]
#[derive(Clone, Debug)]
pub struct BPyDFSchema {
    df_schema: DFSchemaRef,
}
