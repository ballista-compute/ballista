/*
A great deal of this files source code was pulled more or less verbatim from https://github.com/jorgecarleitao/datafusion-python/commit/688f0d23504704cfc2be3fca33e2707e964ea5bc
which is dual liscensed as MIT or Apache-2.0.
*/
/*
MIT License

Copyright (c) 2020 Jorge Leitao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use pyo3::{
    basic::CompareOp, exceptions::PyException, prelude::*, types::PyTuple, PyNumberProtocol,
    PyObjectProtocol,
};

use crate::util;
use datafusion::logical_plan;
use datafusion::logical_plan::Expr;
/// An expression that can be used on a DataFrame
#[pyclass(name = "Expression", module = "ballista")]
#[derive(Debug, Clone)]
pub struct BPyExpr {
    pub(crate) expr: Expr,
}

/// converts a tuple of expressions into a vector of Expressions
pub fn from_tuple(value: &PyTuple) -> PyResult<Vec<BPyExpr>> {
    value
        .iter()
        .map(|e| e.extract::<BPyExpr>())
        .collect::<PyResult<_>>()
}

pub fn any_to_expression(any: &PyAny) -> PyResult<Expr> {
    if let Ok(expr) = any.extract::<BPyExpr>() {
        Ok(expr.expr)
    } else if let Ok(scalar_value) = any.extract::<crate::scalar::Scalar>() {
        Ok(Expr::Literal(scalar_value.scalar))
    } else {
        let type_name = util::object_class_name(any)
            .unwrap_or_else(|err| format!("<Could not get class name:{}>", err));
        Err(PyException::new_err(format!(
            "The rhs type {} could not be converted to an expression.",
            &type_name
        )))
    }
}

#[pyproto]
impl PyNumberProtocol for BPyExpr {
    fn __add__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr + rhs_expr,
        })
    }

    fn __sub__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr - rhs_expr,
        })
    }

    fn __truediv__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr / rhs_expr,
        })
    }

    fn __mul__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr * rhs_expr,
        })
    }

    fn __and__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr.and(rhs_expr),
        })
    }

    fn __or__(lhs: BPyExpr, rhs: &PyAny) -> PyResult<BPyExpr> {
        let rhs_expr = any_to_expression(rhs)?;
        Ok(BPyExpr {
            expr: lhs.expr.or(rhs_expr),
        })
    }

    fn __invert__(&self) -> PyResult<BPyExpr> {
        Ok(BPyExpr {
            expr: self.expr.not(),
        })
    }
}

#[pyproto]
impl PyObjectProtocol for BPyExpr {
    fn __str__(&self) -> String {
        format!("{:?}", self.expr)
    }

    fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> PyResult<BPyExpr> {
        let other_expr = any_to_expression(other)?;
        Ok(match op {
            CompareOp::Lt => BPyExpr {
                expr: self.expr.lt(other_expr),
            },
            CompareOp::Le => BPyExpr {
                expr: self.expr.lt_eq(other_expr),
            },
            CompareOp::Eq => BPyExpr {
                expr: self.expr.eq(other_expr),
            },
            CompareOp::Ne => BPyExpr {
                expr: self.expr.not_eq(other_expr),
            },
            CompareOp::Gt => BPyExpr {
                expr: self.expr.gt(other_expr),
            },
            CompareOp::Ge => BPyExpr {
                expr: self.expr.gt_eq(other_expr),
            },
        })
    }
}

#[pymethods]
impl BPyExpr {
    #[new]
    fn new(expr: &PyAny) -> PyResult<BPyExpr> {
        let converted_expr = any_to_expression(expr)?;
        Ok(BPyExpr {
            expr: converted_expr,
        })
    }

    /// assign a name to the expression
    pub fn alias(&self, name: &str) -> PyResult<BPyExpr> {
        Ok(BPyExpr {
            expr: self.expr.alias(name),
        })
    }
    #[args(negated = "false")]
    pub fn between(&self, low: &PyAny, high: &PyAny, negated: bool) -> PyResult<BPyExpr> {
        let low_expr = any_to_expression(low)?;
        let high_expr = any_to_expression(high)?;

        Ok(BPyExpr {
            expr: Expr::Between {
                expr: Box::new(self.expr.clone()),
                low: Box::new(low_expr),
                high: Box::new(high_expr),
                negated,
            },
        })
    }
}
use pyo3::PyCell;

#[pyclass(module = "ballista", module = "ballista")]
#[derive(Clone)]
///Struct that allows chaining method calls to build CASE WHEN ELSE expressions.
///SAFETY: when method uses PyCell to allow for method chaining without excessive cloning.
pub struct CaseBuilder {
    case_expr: Option<logical_plan::Expr>,
    when_expr: Vec<logical_plan::Expr>,
    then_expr: Vec<logical_plan::Expr>,
    else_expr: Option<logical_plan::Expr>,
    //Acts as flag to mark if expression has already built. If an expression has already built then it can not be added to or extended
    //Additionally all other fields are replaced with garbage when the expression is built.
    built_expr: Option<BPyExpr>,
}

#[pymethods]
impl CaseBuilder {
    #[new]
    #[args(case_expr = "None")]
    pub fn new(case_expr: Option<BPyExpr>) -> Self {
        Self {
            case_expr: case_expr.map(|e| e.expr),
            when_expr: vec![],
            then_expr: vec![],
            else_expr: None,
            built_expr: None,
        }
    }
    ///Add when clause to case expression.
    ///Must only be used before otherwise or build are called
    pub fn when<'a>(
        slf: &'a PyCell<Self>,
        when: &PyAny,
        then: &PyAny,
    ) -> PyResult<&'a PyCell<Self>> {
        {
            let mut __self = slf.try_borrow_mut()?;
            let when_expr = any_to_expression(when)?;
            let then_expr = any_to_expression(then)?;
            __self.when_impl(when_expr, then_expr)?;
        }
        Ok(slf)
    }

    ///Add otherwise clause to case expression.
    ///Builds the case expression, should only be used once all WHEN THEN statements have been added
    pub fn otherwise(&mut self, else_expr: &PyAny) -> PyResult<BPyExpr> {
        let other_size_expr = any_to_expression(else_expr)?;
        self.is_built_error()?;
        self.else_expr = Some(other_size_expr);
        self.private_build()
    }

    ///Builds the CASE expression, once the expression has been built it can not be modified.
    pub fn build(&mut self) -> PyResult<BPyExpr> {
        if self.is_built() {
            return Ok(self.built_expr.as_ref().unwrap().clone());
        }
        self.private_build()
    }

    #[getter]
    pub fn get_expr(&self) -> Option<BPyExpr> {
        self.built_expr.clone()
    }
}

impl CaseBuilder {
    pub fn case(case_expr: Expr) -> Self {
        Self::new(Some(BPyExpr { expr: case_expr }))
    }

    pub fn when_impl(&mut self, when: Expr, then: Expr) -> PyResult<()> {
        self.is_built_error()?;
        self.when_expr.push(when);
        self.then_expr.push(then);
        Ok(())
    }

    fn is_built(&self) -> bool {
        self.built_expr.is_some()
    }

    fn is_built_error(&self) -> PyResult<()> {
        if self.is_built() {
            return Err(PyException::new_err("This case builder has already been used, use 'expr' attribute to access expression or create a new builder using case function"));
        }
        Ok(())
    }
    /// This method may only be called once
    fn private_build(&mut self) -> PyResult<BPyExpr> {
        let mut temp_case = None;
        if self.when_expr.is_empty() {
            return Err(PyException::new_err(
                "The builder must have at least one when then clause added before building",
            ));
        }
        std::mem::swap(&mut temp_case, &mut self.case_expr);
        let mut builder = match temp_case {
            Some(expr) => {
                logical_plan::case(expr).when(self.when_expr.remove(0), self.then_expr.remove(0))
            }
            None => logical_plan::when(self.when_expr.remove(0), self.then_expr.remove(0)),
        };
        let mut temp_when = vec![];
        let mut temp_then = vec![];

        std::mem::swap(&mut temp_when, &mut self.when_expr);
        std::mem::swap(&mut temp_then, &mut self.then_expr);
        for (when, then) in temp_when.into_iter().zip(temp_then.into_iter()) {
            builder = builder.when(when, then);
        }

        let mut temp_else = None;
        std::mem::swap(&mut temp_else, &mut self.else_expr);
        let build_result = match temp_else {
            Some(else_expr) => builder.otherwise(else_expr),
            None => builder.end(),
        }
        .map_err(util::wrap_err)?;
        self.built_expr = Some(BPyExpr { expr: build_result });
        return Ok(self.built_expr.as_ref().unwrap().clone());
    }
}
