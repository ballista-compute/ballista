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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::arrow::array;
use crate::arrow::compute;
use crate::arrow::datatypes::{DataType, Schema};
use crate::cast_array;
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, AggregateMode, ColumnarBatch, ColumnarValue, Expression,
};

#[derive(Debug)]
pub struct Sum {
    input: Arc<dyn Expression>,
}

impl Sum {
    pub fn new(input: Arc<dyn Expression>) -> Self {
        Self { input }
    }
}

impl AggregateExpr for Sum {
    fn name(&self) -> String {
        format!("SUM({:?})", self.input)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.input.data_type(input_schema)? {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            other => Err(ballista_error(&format!(
                "SUM does not support {:?} for {:?} with schema {:?}",
                other, self.input, input_schema
            ))),
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue> {
        self.input.evaluate(batch)
    }

    fn create_accumulator(&self, _mode: &AggregateMode) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(SumAccumulator { sum: None }))
    }
}

macro_rules! accumulate {
    ($SELF:ident, $VALUE:expr, $ARRAY_TYPE:ident, $SCALAR_VARIANT:ident, $TY:ty) => {{
        match $SELF.sum {
            Some(ScalarValue::$SCALAR_VARIANT(n)) => {
                $SELF.sum = Some(ScalarValue::$SCALAR_VARIANT(n + $VALUE as $TY));
            }
            Some(_) => return Err(ballista_error("Unexpected ScalarValue variant")),
            None => {}
        };
    }};
}

pub struct SumAccumulator {
    pub sum: Option<ScalarValue>,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()> {
        match value {
            ColumnarValue::Columnar(array) => {
                let sum = match array.data_type() {
                    DataType::UInt8 => match compute::sum(cast_array!(array, UInt8Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                        None => Ok(None),
                    },
                    DataType::UInt16 => match compute::sum(cast_array!(array, UInt16Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                        None => Ok(None),
                    },
                    DataType::UInt32 => match compute::sum(cast_array!(array, UInt32Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                        None => Ok(None),
                    },
                    DataType::UInt64 => match compute::sum(cast_array!(array, UInt64Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                        None => Ok(None),
                    },
                    DataType::Int8 => match compute::sum(cast_array!(array, Int8Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int8(n))),
                        None => Ok(None),
                    },
                    DataType::Int16 => match compute::sum(cast_array!(array, Int16Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int16(n))),
                        None => Ok(None),
                    },
                    DataType::Int32 => match compute::sum(cast_array!(array, Int32Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int32(n))),
                        None => Ok(None),
                    },
                    DataType::Int64 => match compute::sum(cast_array!(array, Int64Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int64(n))),
                        None => Ok(None),
                    },
                    DataType::Float32 => match compute::sum(cast_array!(array, Float32Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Float32(n))),
                        None => Ok(None),
                    },
                    DataType::Float64 => match compute::sum(cast_array!(array, Float64Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Float64(n))),
                        None => Ok(None),
                    },
                    _ => Err(ballista_error("Unsupported data type for SUM")),
                }?;
                self.accumulate(&ColumnarValue::Scalar(sum, 1))?;
            }
            ColumnarValue::Scalar(value, _) => {
                if let Some(value) = value {
                    match value {
                        ScalarValue::Int8(value) => {
                            accumulate!(self, *value, Int8Array, Int64, i64);
                        }
                        ScalarValue::Int16(value) => {
                            accumulate!(self, *value, Int16Array, Int64, i64);
                        }
                        ScalarValue::Int32(value) => {
                            accumulate!(self, *value, Int32Array, Int64, i64);
                        }
                        ScalarValue::Int64(value) => {
                            accumulate!(self, *value, Int64Array, Int64, i64);
                        }
                        ScalarValue::UInt8(value) => {
                            accumulate!(self, *value, UInt8Array, UInt64, u64);
                        }
                        ScalarValue::UInt16(value) => {
                            accumulate!(self, *value, UInt16Array, UInt64, u64);
                        }
                        ScalarValue::UInt32(value) => {
                            accumulate!(self, *value, UInt32Array, UInt64, u64);
                        }
                        ScalarValue::UInt64(value) => {
                            accumulate!(self, *value, UInt64Array, UInt64, u64);
                        }
                        ScalarValue::Float32(value) => {
                            accumulate!(self, *value, Float32Array, Float32, f32);
                        }
                        ScalarValue::Float64(value) => {
                            accumulate!(self, *value, Float64Array, Float64, f64);
                        }
                        other => {
                            return Err(ballista_error(&format!(
                                "SUM does not support {:?}",
                                other
                            )))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(self.sum.clone())
    }
}

/// Create a sum expression
pub fn sum(expr: Arc<dyn Expression>) -> Arc<dyn AggregateExpr> {
    Arc::new(Sum::new(expr))
}
