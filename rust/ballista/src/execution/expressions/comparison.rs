// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use crate::arrow::array;
use crate::arrow::compute;
use crate::arrow::datatypes::{DataType, Schema};
use crate::cast_array;
use crate::datafusion::logicalplan::Operator;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};

#[derive(Debug)]
pub struct Comparison {
    l: Arc<dyn Expression>,
    op: Operator,
    r: Arc<dyn Expression>,
}

impl Comparison {
    pub fn new(l: Arc<dyn Expression>, op: Operator, r: Arc<dyn Expression>) -> Self {
        Self { l, op, r }
    }
}

pub fn compare(
    l: Arc<dyn Expression>,
    op: &Operator,
    r: Arc<dyn Expression>,
) -> Arc<dyn Expression> {
    Arc::new(Comparison::new(l, op.to_owned(), r))
}

macro_rules! compare_op {
    ($LEFT:ident, $RIGHT:ident, $TY:ident, $OP:expr) => {{
        let l = cast_array!($LEFT, $TY)?;
        let r = cast_array!($RIGHT, $TY)?;
        let bools = match $OP {
            Operator::Lt => Ok(compute::lt(l, r)?),
            Operator::LtEq => Ok(compute::lt_eq(l, r)?),
            Operator::Gt => Ok(compute::gt(l, r)?),
            Operator::GtEq => Ok(compute::gt_eq(l, r)?),
            Operator::Eq => Ok(compute::eq(l, r)?),
            Operator::NotEq => Ok(compute::neq(l, r)?),
            other => Err(ballista_error(&format!(
                "Invalid comparison operator '{:?}'",
                other
            ))),
        }?;
        Ok(ColumnarValue::Columnar(Arc::new(bools)))
    }};
}

impl Expression for Comparison {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow()?;
        let r = self.r.evaluate(input)?.to_arrow()?;
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Comparison expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int8 => compare_op!(l, r, Int8Array, &self.op),
            DataType::Int16 => compare_op!(l, r, Int16Array, &self.op),
            DataType::Int32 => compare_op!(l, r, Int32Array, &self.op),
            DataType::Int64 => compare_op!(l, r, Int64Array, &self.op),
            DataType::UInt8 => compare_op!(l, r, UInt8Array, &self.op),
            DataType::UInt16 => compare_op!(l, r, UInt16Array, &self.op),
            DataType::UInt32 => compare_op!(l, r, UInt32Array, &self.op),
            DataType::UInt64 => compare_op!(l, r, UInt64Array, &self.op),
            DataType::Float32 => compare_op!(l, r, Float32Array, &self.op),
            DataType::Float64 => compare_op!(l, r, Float64Array, &self.op),
            DataType::Utf8 => {
                let l = cast_array!(l, StringArray)?;
                let r = cast_array!(r, StringArray)?;
                let bools = match &self.op {
                    Operator::Lt => Ok(compute::lt_utf8(l, r)?),
                    Operator::LtEq => Ok(compute::lt_eq_utf8(l, r)?),
                    Operator::Gt => Ok(compute::gt_utf8(l, r)?),
                    Operator::GtEq => Ok(compute::gt_eq_utf8(l, r)?),
                    Operator::Eq => Ok(compute::eq_utf8(l, r)?),
                    Operator::NotEq => Ok(compute::neq_utf8(l, r)?),
                    other => Err(ballista_error(&format!(
                        "Invalid comparison operator '{:?}'",
                        other
                    ))),
                }?;
                Ok(ColumnarValue::Columnar(Arc::new(bools)))
            }
            _ => Err(ballista_error(
                "Unsupported datatype for Comparison expression",
            )),
        }
    }
}
