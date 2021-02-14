#![deny(clippy::all)]
pub mod context;
pub mod dataframe;
pub mod datatypes;
pub mod dfschema;
pub mod error;
pub mod expression;
pub mod field;
pub mod functions;
pub mod partition;
pub mod scalar;
pub mod schema;
pub mod util;

use pyo3::prelude::*;
//How to start tokio runtime

#[pymodule]
fn ballista(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<context::BPyBallistaContext>()?;
    m.add_class::<expression::BPyExpr>()?;
    m.add_class::<dataframe::BPyDataFrame>()?;
    m.add_class::<expression::CaseBuilder>()?;
    m.add_class::<schema::BPySchema>()?;
    m.add_class::<field::BPyField>()?;
    m.add_class::<partition::BPyPartitioning>()?;
    m.add_class::<dfschema::BPyDFSchema>()?;

    crate::functions::init(m)?;
    datatypes::init(m)?;

    Ok(())
}
