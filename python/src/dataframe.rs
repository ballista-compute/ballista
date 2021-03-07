use crate::expression::BPyExpr;
use arrow::record_batch::RecordBatch;
use ballista::context::BallistaDataFrame;
use datafusion::logical_plan::Expr;
use futures::StreamExt;
use pyo3::exceptions::PyException;
use pyo3::{ffi::Py_uintptr_t, prelude::*};

use crate::{expression::any_to_expression, util};
use pyo3::PyErr;

use std::convert::From;

use arrow::array::ArrayRef;

#[pyclass(unsendable, module = "ballista", name = "DataFrame")]
pub struct BPyDataFrame {
    pub df: BallistaDataFrame,
}

use crate::dfschema::BPyDFSchema;
use pyo3::types::{PyList, PyTuple};

#[pymethods]
impl BPyDataFrame {
    #[args(columns = "*")]
    pub fn select_columns(&self, columns: &PyTuple) -> PyResult<BPyDataFrame> {
        let col_vec: Vec<&str> = util::tuple_to_uniform_type(columns)?;
        let df = self
            .df
            .select_columns(col_vec.as_slice())
            .map_err(util::wrap_err)?;
        let ballista_df = BPyDataFrame { df };
        Ok(ballista_df)
    }

    #[args(expr = "*")]
    fn select(&self, expr: &PyTuple) -> PyResult<Self> {
        let expressions: Vec<Expr> =
            util::transform_tuple_to_uniform_type(expr, |e: BPyExpr| e.expr)?;
        self.df
            .select(expressions.as_slice())
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }

    #[args(expr = "*")]
    fn filter(&self, expr: &PyAny) -> PyResult<Self> {
        let filter_expr = any_to_expression(expr)?;
        self.df
            .filter(filter_expr)
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }

    fn aggregate(&self, group_expr: &PyList, aggr_expr: &PyList) -> PyResult<Self> {
        let group_by = group_expr
            .iter()
            .map(|any| any_to_expression(any))
            .collect::<PyResult<Vec<Expr>>>()?;
        let aggr_by = aggr_expr
            .iter()
            .map(|any| any_to_expression(any))
            .collect::<PyResult<Vec<Expr>>>()?;
        self.df
            .aggregate(group_by.as_slice(), aggr_by.as_slice())
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }

    fn limit(&self, n: usize) -> PyResult<Self> {
        self.df.limit(n).map(|df| df.into()).map_err(util::wrap_err)
    }

    fn sort(&self, expr: &PyTuple) -> PyResult<Self> {
        let expressions = util::transform_tuple_to_uniform_type(expr, |e: BPyExpr| e.expr)?;
        self.df
            .sort(expressions.as_slice())
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }
    //TODO: add join implementation to dataframe, blocked on BallistaDataFrame adding join
    #[allow(unused_variables)]
    #[args(join_type = "\"inner\"")]
    fn join(
        slf: &PyCell<BPyDataFrame>,
        right: &PyCell<BPyDataFrame>,
        left_cols: &PyList,
        right_cols: &PyList,
        join_type: &str,
    ) -> PyResult<Self> {
        Err(PyException::new_err(
            "Not implemented on ballista dataframe yet",
        ))
    }

    fn repartition(
        &self,
        partitioning_scheme: crate::partition::BPyPartitioning,
    ) -> PyResult<Self> {
        self.df
            .repartition(partitioning_scheme.scheme)
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }

    fn collect(&self, _py: Python) -> PyResult<PyObject> {
        let rt = tokio::runtime::Runtime::new().map_err(util::wrap_err)?;
        let batches: Vec<RecordBatch> =
            rt.block_on(self.async_collect()).map_err(util::wrap_err)?;
        to_py(&batches)
    }

    fn schema(&self) -> BPyDFSchema {
        self.df.schema().clone().into()
    }
    #[args(verbose = "false")]
    fn explain(&self, verbose: bool) -> PyResult<Self> {
        self.df
            .explain(verbose)
            .map(|df| df.into())
            .map_err(util::wrap_err)
    }
}

pub fn to_py(batches: &[RecordBatch]) -> PyResult<PyObject> {
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();
    let pyarrow = PyModule::import(py, "pyarrow")?;
    let builtins = PyModule::import(py, "builtins")?;

    let mut py_batches = vec![];
    for batch in batches {
        py_batches.push(to_py_batch(batch, py, pyarrow)?);
    }
    let result = builtins.call1("list", (py_batches,))?;
    Ok(PyObject::from(result))
}

pub fn to_py_array(array: &ArrayRef, py: Python) -> PyResult<PyObject> {
    let (array_pointer, schema_pointer) = array.to_raw().map_err(crate::util::wrap_err)?;

    let pa = py.import("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (
            array_pointer as Py_uintptr_t,
            schema_pointer as Py_uintptr_t,
        ),
    )?;
    Ok(array.to_object(py))
}

fn to_py_batch<'a>(
    batch: &RecordBatch,
    py: Python,
    pyarrow: &'a PyModule,
) -> Result<PyObject, PyErr> {
    let mut py_arrays = vec![];
    let mut py_names = vec![];

    let schema = batch.schema();
    for (array, field) in batch.columns().iter().zip(schema.fields().iter()) {
        let array = to_py_array(array, py)?;

        py_arrays.push(array);
        py_names.push(field.name());
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (py_arrays, py_names))?;

    Ok(PyObject::from(record))
}

impl BPyDataFrame {
    async fn async_collect<'py>(&self) -> PyResult<Vec<RecordBatch>> {
        let mut stream = self.df.collect().await.map_err(wrap_err)?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(wrap_err)?;
            batches.push(batch);
        }
        Ok(batches)
    }
}

impl From<ballista_core::context::BallistaDataFrame> for BPyDataFrame {
    fn from(df: ballista_core::context::BallistaDataFrame) -> BPyDataFrame {
        BPyDataFrame { df }
    }
}

pub fn wrap_err<E: std::error::Error>(err: E) -> PyErr {
    PyException::new_err(err.to_string())
}
