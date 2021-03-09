use crate::expression::{self, BPyExpr};
use crate::scalar::Scalar;
use crate::util;
use datafusion::logical_plan;
use datafusion::logical_plan::Expr;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[pyfunction]
pub fn col(name: &str) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::col(name),
    }
}

#[pyfunction(module = "ballista")]
pub fn lit(literal: &PyAny) -> PyResult<BPyExpr> {
    let literal_value = literal.extract::<Scalar>()?;
    Ok(BPyExpr {
        expr: Expr::Literal(literal_value.scalar),
    })
}

#[pyfunction]
pub fn sum(value: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::sum(value.expr),
    }
}

#[pyfunction]
pub fn avg(value: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::avg(value.expr),
    }
}

#[pyfunction]
pub fn min(value: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::min(value.expr),
    }
}

#[pyfunction]
pub fn max(value: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::max(value.expr),
    }
}

#[pyfunction]
pub fn count(value: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::count(value.expr),
    }
}

#[pyfunction]
pub fn count_distinct(expr: BPyExpr) -> BPyExpr {
    BPyExpr {
        expr: logical_plan::count_distinct(expr.expr),
    }
}

#[pyfunction(expr)]
pub fn concat(expr: BPyExpr) -> PyResult<BPyExpr> {
    Ok(BPyExpr {
        expr: logical_plan::concat(expr.expr),
    })
}

#[pyfunction]
pub fn when(when: &PyAny, then: &PyAny) -> PyResult<expression::CaseBuilder> {
    let mut builder = expression::CaseBuilder::new(None);
    //when method only fails when CaseBuilder has already built the expression so this is safe to unwrap
    let when_expr = expression::any_to_expression(when)?;
    let then_expr = expression::any_to_expression(then)?;
    builder.when_impl(when_expr, then_expr).unwrap();
    Ok(builder)
}

#[pyfunction]
pub fn case(case: &PyAny) -> PyResult<expression::CaseBuilder> {
    let case_expr = expression::any_to_expression(case)?;
    Ok(expression::CaseBuilder::case(case_expr))
}

#[pyfunction(args = "*")]
pub fn array(args: &PyTuple) -> PyResult<BPyExpr> {
    let args: Vec<Expr> = util::transform_tuple_to_uniform_type(args, |e: BPyExpr| e.expr)?;
    Ok(BPyExpr {
        expr: logical_plan::array(args),
    })
}

#[macro_export]
macro_rules! unary_scalar_expr_pyfunction {
    ($FUNC:ident) => {
        #[pyfunction]
        pub fn $FUNC(e: BPyExpr) -> BPyExpr {
            use datafusion::logical_plan;
            BPyExpr {
                expr: logical_plan::$FUNC(e.expr),
            }
        }
    };
}

//Generate functions for unary functions
unary_scalar_expr_pyfunction!(sqrt);
unary_scalar_expr_pyfunction!(sin);
unary_scalar_expr_pyfunction!(cos);
unary_scalar_expr_pyfunction!(tan);
unary_scalar_expr_pyfunction!(asin);
unary_scalar_expr_pyfunction!(acos);
unary_scalar_expr_pyfunction!(atan);
unary_scalar_expr_pyfunction!(floor);
unary_scalar_expr_pyfunction!(ceil);
unary_scalar_expr_pyfunction!(round);
unary_scalar_expr_pyfunction!(trunc);
unary_scalar_expr_pyfunction!(abs);
unary_scalar_expr_pyfunction!(signum);
unary_scalar_expr_pyfunction!(exp);
unary_scalar_expr_pyfunction!(ln);
unary_scalar_expr_pyfunction!(log2);
unary_scalar_expr_pyfunction!(log10);
unary_scalar_expr_pyfunction!(lower);
unary_scalar_expr_pyfunction!(trim);
unary_scalar_expr_pyfunction!(upper);

use pyo3::wrap_pyfunction;

pub fn init(module: &PyModule) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    module.add_function(wrap_pyfunction!(avg, module)?)?;
    module.add_function(wrap_pyfunction!(min, module)?)?;
    module.add_function(wrap_pyfunction!(max, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(count_distinct, module)?)?;
    module.add_function(wrap_pyfunction!(concat, module)?)?;
    module.add_function(wrap_pyfunction!(when, module)?)?;
    module.add_function(wrap_pyfunction!(case, module)?)?;
    module.add_function(wrap_pyfunction!(array, module)?)?;

    //All macro generated scalar functions
    module.add_function(wrap_pyfunction!(sqrt, module)?)?;
    module.add_function(wrap_pyfunction!(sin, module)?)?;
    module.add_function(wrap_pyfunction!(cos, module)?)?;
    module.add_function(wrap_pyfunction!(tan, module)?)?;
    module.add_function(wrap_pyfunction!(asin, module)?)?;
    module.add_function(wrap_pyfunction!(acos, module)?)?;
    module.add_function(wrap_pyfunction!(atan, module)?)?;
    module.add_function(wrap_pyfunction!(floor, module)?)?;
    module.add_function(wrap_pyfunction!(ceil, module)?)?;
    module.add_function(wrap_pyfunction!(round, module)?)?;
    module.add_function(wrap_pyfunction!(trunc, module)?)?;
    module.add_function(wrap_pyfunction!(abs, module)?)?;
    module.add_function(wrap_pyfunction!(signum, module)?)?;
    module.add_function(wrap_pyfunction!(exp, module)?)?;
    module.add_function(wrap_pyfunction!(ln, module)?)?;
    module.add_function(wrap_pyfunction!(log2, module)?)?;
    module.add_function(wrap_pyfunction!(log10, module)?)?;
    module.add_function(wrap_pyfunction!(lower, module)?)?;
    module.add_function(wrap_pyfunction!(trim, module)?)?;
    module.add_function(wrap_pyfunction!(upper, module)?)?;

    Ok(())
}
