use crate::expression::BPyExpr;
use crate::util;
use datafusion::logical_plan::Partitioning;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
#[pyclass(name = "Partitioning", module = "ballista")]
#[derive(Clone)]
pub struct BPyPartitioning {
    pub(crate) scheme: Partitioning,
}

#[pymethods]
impl BPyPartitioning {
    #[staticmethod]
    #[args(exprs = "*")]
    fn hash(n: usize, exprs: &PyTuple) -> PyResult<Self> {
        let hash_exprs = util::transform_tuple_to_uniform_type(exprs, |e: BPyExpr| e.expr)?;
        Ok(BPyPartitioning {
            scheme: Partitioning::Hash(hash_exprs, n),
        })
    }

    #[staticmethod]
    fn round_robin(n: usize) -> Self {
        BPyPartitioning {
            scheme: Partitioning::RoundRobinBatch(n),
        }
    }
}

use pyo3::basic::PyObjectProtocol;
#[pyproto]
impl PyObjectProtocol for BPyPartitioning {
    fn __str__(&self) -> String {
        format!("{:?}", self.scheme)
    }
}
