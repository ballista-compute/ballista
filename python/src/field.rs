use crate::datatypes;
use crate::util;
use arrow::datatypes::Field;
use datafusion::logical_plan::DFField;
use datatypes::any_to_datatype;
use pyo3::types::PyDict;
use pyo3::{exceptions::PyException, prelude::*};
//Fills the roles of both arrow::Field role and datafusion::DFField
#[pyclass(name = "Field", module = "ballista")]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BPyField {
    #[serde(flatten)]
    pub(crate) arrow_field: Field,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) qualifier: Option<String>,
}
/*
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<BTreeMap<String, String>>,
}
*/

impl Into<Field> for BPyField {
    fn into(self) -> Field {
        self.arrow_field
    }
}

pub fn any_to_bpyfield(any: &PyAny) -> PyResult<BPyField> {
    let field: BPyField = if let Ok(native_field) = any.extract::<BPyField>() {
        native_field
    } else if let Ok(py_dict) = any.extract::<&PyDict>() {
        //Begin with extracting all required keys from dictionary
        let name = py_dict
            .get_item("name")
            .ok_or_else(|| util::missing_key("name", "field dictionary"))?;
        let datatype = py_dict
            .get_item("datatype")
            .ok_or_else(|| util::missing_key("datatype", "field dictionary"))?;
        //If nullable is not present then set nullable to false
        let nullable = match py_dict.get_item("nullable") {
            Some(pyobj) => pyobj.extract::<bool>()?,
            None => false,
        };

        let dict_id = match py_dict.get_item("dict_id") {
            Some(pyobj) => Some(pyobj.extract::<i64>()?),
            None => None,
        };

        let dict_is_ordered = match py_dict.get_item("dict_is_ordered") {
            Some(pyobj) => pyobj.extract::<bool>()?,
            None => false,
        };

        let qualifier: Option<String> =
            py_dict.get_item("qualifier").map(|pyobj| pyobj.to_string());

        let field_name = name.to_string();
        let field_datatype = any_to_datatype(datatype)?;
        let arrow_field = match dict_id {
            Some(id) => Field::new_dict(
                field_name.as_str(),
                field_datatype,
                nullable,
                id,
                dict_is_ordered,
            ),
            None => Field::new(field_name.as_str(), field_datatype, nullable),
        };
        BPyField {
            arrow_field,
            qualifier,
        }
    } else {
        return Err(PyException::new_err("Could not convert to Field class"));
    };
    Ok(field)
}

impl Into<DFField> for BPyField {
    fn into(self) -> DFField {
        DFField::new(
            self.qualifier.as_deref(),
            self.arrow_field.name().as_str(),
            self.arrow_field.data_type().clone(),
            self.arrow_field.is_nullable(),
        )
    }
}

#[pymethods]
impl BPyField {
    #[new]
    #[args(nullable = "false", qualifier = "None")]
    fn new(
        name: &str,
        datatype: &PyAny,
        nullable: bool,
        qualifier: Option<&str>,
    ) -> PyResult<Self> {
        let native_datatype = datatypes::any_to_datatype(datatype)?;
        let field_qualifier = qualifier.map(|s| s.to_owned());
        Ok(BPyField {
            arrow_field: Field::new(name, native_datatype, nullable),
            qualifier: field_qualifier,
        })
    }

    fn is_nullable(&self) -> bool {
        self.arrow_field.is_nullable()
    }

    fn datatype(&self) -> datatypes::BPyDataType {
        datatypes::BPyDataType {
            datatype: self.arrow_field.data_type().clone(),
        }
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let val: BPyField = serde_json::from_str(json).map_err(util::wrap_err)?;
        Ok(val)
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(util::wrap_err)
    }
}

use pyo3::basic::PyObjectProtocol;
#[pyproto]
impl PyObjectProtocol for BPyField {
    fn __str__(&self) -> String {
        format!(
            "Field{{ name: {}, datatype: {}, nullable: {}}}",
            self.arrow_field.name().as_str(),
            self.arrow_field.data_type(),
            self.arrow_field.is_nullable()
        )
    }
}
