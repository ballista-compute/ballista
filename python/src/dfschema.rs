use crate::field::BPyField;
use crate::util;
use crate::{field::any_to_bpyfield, schema::BPySchema};

use datafusion::logical_plan::{DFField, DFSchema};
use pyo3::types::PyTuple;
use pyo3::{exceptions::PyException, prelude::*};

#[pyclass(name = "DFSchema", module = "ballista")]
pub struct BPyDFSchema {
    schema: DFSchema,
}

impl From<DFSchema> for BPyDFSchema {
    fn from(schema: DFSchema) -> Self {
        BPyDFSchema { schema }
    }
}

#[pymethods]
impl BPyDFSchema {
    //This new method has three distinct calling methods, when both qualifier and schema are None then the schema is constructed by
    //the list of fields in *args. The second, when both qualifier and schema are
    #[new]
    #[args(schema = "None", qualifier = "None", args = "*")]
    fn new(
        qualifier: Option<&str>,
        schema: Option<BPySchema>,
        args: &PyTuple,
    ) -> PyResult<BPyDFSchema> {
        let df_schema: DFSchema = match (qualifier, schema) {
            (Some(qual), Some(sch)) => {
                if !args.is_empty() {
                    return Err(PyException::new_err("When a qualifier and schema are supplied no additional fields are allowd in *args"));
                }
                DFSchema::try_from_qualified(qual, &sch.schema).map_err(util::wrap_err)?
            }
            (Some(_), None) => {
                return Err(PyException::new_err(
                    "When passing a qualifier a schema must also be supplied",
                ))
            }
            (None, Some(_)) => {
                return Err(PyException::new_err(
                    "When passing a schema a qualifier must also be supplied",
                ))
            }
            (None, None) => {
                if args.is_empty() {
                    DFSchema::empty()
                } else {
                    let py_fields: Vec<BPyField> = args
                        .iter()
                        .map(|arg| any_to_bpyfield(arg))
                        .collect::<Result<Vec<_>, _>>()?;
                    let fields: Vec<DFField> = py_fields
                        .into_iter()
                        .map(|py_field| {
                            DFField::new(
                                py_field.qualifier.as_deref(),
                                py_field.arrow_field.name(),
                                py_field.arrow_field.data_type().clone(),
                                py_field.arrow_field.is_nullable(),
                            )
                        })
                        .collect();
                    DFSchema::new(fields).map_err(util::wrap_err)?
                }
            }
        };
        Ok(BPyDFSchema::from(df_schema))
    }

    fn join(&self, schema: &PyCell<BPyDFSchema>) -> PyResult<BPyDFSchema> {
        let bschema = schema.borrow();
        let jschema = self.schema.join(&bschema.schema).map_err(util::wrap_err)?;
        Ok(jschema.into())
    }
}
