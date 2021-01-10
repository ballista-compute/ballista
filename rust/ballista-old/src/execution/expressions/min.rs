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
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, AggregateMode, ColumnarBatch, ColumnarValue, Expression,
};

/// MIN aggregate expression
#[derive(Debug)]
pub struct Min {
    expr: Arc<dyn Expression>,
}

impl Min {
    /// Create a new MIN aggregate function
    pub fn new(expr: Arc<dyn Expression>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for Min {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.expr.data_type(input_schema)? {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            other => Err(ballista_error(&format!("MIN does not support {:?}", other))),
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue> {
        self.expr.evaluate(batch)
    }

    fn create_accumulator(&self, _mode: &AggregateMode) -> Box<dyn Accumulator> {
        // the accumulator for MIN is always MIN regardless of the aggregation mode
        Box::new(MinAccumulator { min: None })
    }
}

macro_rules! min_accumulate {
    ($SELF:ident, $VALUE:expr, $ARRAY_TYPE:ident, $SCALAR_VARIANT:ident, $TY:ty) => {{
        $SELF.min = match $SELF.min {
            Some(ScalarValue::$SCALAR_VARIANT(n)) => {
                if n < (*$VALUE as $TY) {
                    Some(ScalarValue::$SCALAR_VARIANT(n))
                } else {
                    Some(ScalarValue::$SCALAR_VARIANT(*$VALUE as $TY))
                }
            }
            Some(_) => return Err(ballista_error("Unexpected ScalarValue variant")),
            None => Some(ScalarValue::$SCALAR_VARIANT(*$VALUE as $TY)),
        };
    }};
}
struct MinAccumulator {
    min: Option<ScalarValue>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()> {
        match value {
            ColumnarValue::Columnar(array) => {
                let min = match array.data_type() {
                    DataType::UInt8 => match compute::min(cast_array!(array, UInt8Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                        None => Ok(None),
                    },
                    DataType::UInt16 => match compute::min(cast_array!(array, UInt16Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                        None => Ok(None),
                    },
                    DataType::UInt32 => match compute::min(cast_array!(array, UInt32Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                        None => Ok(None),
                    },
                    DataType::UInt64 => match compute::min(cast_array!(array, UInt64Array)?) {
                        Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                        None => Ok(None),
                    },
                    DataType::Int8 => match compute::min(cast_array!(array, Int8Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int8(n))),
                        None => Ok(None),
                    },
                    DataType::Int16 => match compute::min(cast_array!(array, Int16Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int16(n))),
                        None => Ok(None),
                    },
                    DataType::Int32 => match compute::min(cast_array!(array, Int32Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int32(n))),
                        None => Ok(None),
                    },
                    DataType::Int64 => match compute::min(cast_array!(array, Int64Array)?) {
                        Some(n) => Ok(Some(ScalarValue::Int64(n))),
                        None => Ok(None),
                    },
                    DataType::Float32 => {
                        match compute::min(
                            array
                                .as_any()
                                .downcast_ref::<array::Float32Array>()
                                .unwrap(),
                        ) {
                            Some(n) => Ok(Some(ScalarValue::Float32(n))),
                            None => Ok(None),
                        }
                    }
                    DataType::Float64 => {
                        match compute::min(
                            array
                                .as_any()
                                .downcast_ref::<array::Float64Array>()
                                .unwrap(),
                        ) {
                            Some(n) => Ok(Some(ScalarValue::Float64(n))),
                            None => Ok(None),
                        }
                    }
                    _ => Err(ballista_error("Unsupported data type for MIN")),
                }?;
                if let Some(min) = min {
                    self.accumulate(&ColumnarValue::Scalar(min, 1))
                } else {
                    Ok(())
                }
            }
            ColumnarValue::Scalar(value, _n) => {
                match value {
                    ScalarValue::Int8(value) => {
                        min_accumulate!(self, value, Int8Array, Int64, i64);
                    }
                    ScalarValue::Int16(value) => {
                        min_accumulate!(self, value, Int16Array, Int64, i64)
                    }
                    ScalarValue::Int32(value) => {
                        min_accumulate!(self, value, Int32Array, Int64, i64)
                    }
                    ScalarValue::Int64(value) => {
                        min_accumulate!(self, value, Int64Array, Int64, i64)
                    }
                    ScalarValue::UInt8(value) => {
                        min_accumulate!(self, value, UInt8Array, UInt64, u64)
                    }
                    ScalarValue::UInt16(value) => {
                        min_accumulate!(self, value, UInt16Array, UInt64, u64)
                    }
                    ScalarValue::UInt32(value) => {
                        min_accumulate!(self, value, UInt32Array, UInt64, u64)
                    }
                    ScalarValue::UInt64(value) => {
                        min_accumulate!(self, value, UInt64Array, UInt64, u64)
                    }
                    ScalarValue::Float32(value) => {
                        min_accumulate!(self, value, Float32Array, Float32, f32)
                    }
                    ScalarValue::Float64(value) => {
                        min_accumulate!(self, value, Float64Array, Float64, f64)
                    }
                    other => {
                        return Err(ballista_error(&format!("MIN does not support {:?}", other)))
                    }
                }
                Ok(())
            }
        }
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(self.min.clone())
    }
}

/// Create a min expression
pub fn min(expr: Arc<dyn Expression>) -> Arc<dyn AggregateExpr> {
    Arc::new(Min::new(expr))
}
