use datafusion::prelude::CsvReadOptions;
use std::collections::HashMap;

use crate::dataframe::{wrap_err, BPyDataFrame};
use crate::schema::BPySchema;
use pyo3::types::PyTuple;
use pyo3::{exceptions::PyException, prelude::*};
#[pyclass(unsendable, name = "BallistaContext", module = "ballista")]
pub(crate) struct BPyBallistaContext {
    ctx: ballista::context::BallistaContext,
}

pub fn wrap_df(df: ballista::context::BallistaDataFrame) -> BPyDataFrame {
    BPyDataFrame { df }
}

#[pymethods]
impl BPyBallistaContext {
    #[new]
    #[args(host = "\"localhost\"", port = "50051", kwds = "**")]
    pub fn new(host: &str, port: u16, kwds: Option<&pyo3::types::PyDict>) -> PyResult<Self> {
        let settings = match kwds {
            Some(kwargs) => kwargs
                .iter()
                .map(|(py_key_obj, py_val_obj)| {
                    let py_key = py_key_obj.str().map_err(|err| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "kwargs keys must be convertible to a string using __str__: {}",
                            err.to_string()
                        ))
                    })?;
                    let py_val = py_val_obj.str().map_err(|err| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "kwargs values must be convertible to a string using __str__: {}",
                            err.to_string()
                        ))
                    })?;
                    Ok((py_key.to_str()?.to_owned(), py_val.to_str()?.to_owned()))
                })
                .collect::<Result<HashMap<String, String>, PyErr>>()?,
            None => HashMap::new(),
        };
        Ok(BPyBallistaContext {
            ctx: ballista::context::BallistaContext::remote(host, port, settings),
        })
    }

    pub fn read_parquet(&self, path: &str) -> PyResult<BPyDataFrame> {
        let ballista_df = self.ctx.read_parquet(path).map_err(wrap_err)?;
        Ok(BPyDataFrame { df: ballista_df })
    }

    //#[args(has_header="true", delimiter="\',\'", file_extension="\"csv\"")]
    #[allow(clippy::clippy::too_many_arguments)]
    pub fn read_csv(
        &self,
        path: &str,
        has_header: bool,
        delimiter: char,
        file_extension: &str,
        schema: Option<BPySchema>,
        schema_infer_max_records: usize,
    ) -> PyResult<BPyDataFrame> {
        let delimiter_byte = if delimiter.is_ascii() {
            delimiter as u8
        } else {
            return Err(PyException::new_err(format!(
                "Invalid delimiter {}, currently ballista only accepts ascii as csv delimiters",
                delimiter
            )));
        };
        let options: CsvReadOptions = CsvReadOptions {
            has_header,
            delimiter: delimiter_byte,
            schema: schema.as_ref().map(|schema| &schema.schema),
            schema_infer_max_records,
            file_extension,
        };
        self.ctx
            .read_csv(path, options)
            .map(wrap_df)
            .map_err(wrap_err)
    }

    pub fn register_table(&self, name: &str, table: &BPyDataFrame) -> PyResult<()> {
        self.ctx.register_table(name, &table.df).map_err(wrap_err)
    }

    #[args(
        name,
        path,
        _py_args = "*",
        delimiter = "','",
        hasheader = "false",
        file_extension = "\".csv\"",
        schema = "None",
        schema_infer_max_records = "100"
    )]
    #[allow(clippy::clippy::too_many_arguments)]
    pub fn register_csv(
        &self,
        name: &str,
        path: &str,
        _py_args: &PyTuple,
        has_header: bool,
        delimiter: char,
        file_extension: &str,
        schema: Option<BPySchema>,
        schema_infer_max_records: usize,
    ) -> PyResult<()> {
        if name.is_empty() {
            return Err(PyException::new_err("Must provide table name"));
        }
        let delimiter_byte = if delimiter.is_ascii() {
            delimiter as u8
        } else {
            return Err(PyException::new_err(format!(
                "Invalid delimiter {}, currently ballista only accepts ascii as csv delimiters",
                delimiter
            )));
        };
        let mut options = CsvReadOptions::new()
            .delimiter(delimiter_byte)
            .has_header(has_header)
            .file_extension(file_extension);
        options = match &schema {
            Some(schema) => options.schema(&schema.schema),
            None => options.schema_infer_max_records(schema_infer_max_records),
        };
        self.ctx.register_csv(name, path, options).map_err(wrap_err)
    }

    pub fn register_parquet(&self, name: &str, path: &str) -> PyResult<()> {
        self.ctx.register_parquet(name, path).map_err(wrap_err)
    }

    pub fn sql(&self, sql: &str) -> PyResult<BPyDataFrame> {
        self.ctx.sql(sql).map(wrap_df).map_err(wrap_err)
    }
}
