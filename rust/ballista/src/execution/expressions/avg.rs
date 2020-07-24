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

#[derive(Debug)]
pub struct Avg {
    input: Arc<dyn Expression>,
}

impl Avg {
    pub fn new(input: Arc<dyn Expression>) -> Self {
        Self { input }
    }
}

impl AggregateExpr for Avg {
    fn name(&self) -> String {
        format!("AVG({:?})", self.input)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.input.data_type(input_schema)? {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => Ok(DataType::Float64),
            other => Err(ballista_error(&format!("AVG does not support {:?}", other))),
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue> {
        self.input.evaluate(batch)
    }

    fn create_accumulator(&self, _mode: &AggregateMode) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator {
            sum: None,
            count: None,
        })
    }
}

macro_rules! avg_accumulate {
    ($SELF:ident, $VALUE:expr, $ARRAY_TYPE:ident) => {{
        match ($SELF.sum, $SELF.count) {
            (Some(sum), Some(count)) => {
                $SELF.sum = Some(sum + $VALUE as f64);
                $SELF.count = Some(count + 1);
            }
            _ => {
                $SELF.sum = Some($VALUE as f64);
                $SELF.count = Some(1);
            }
        };
    }};
}
struct AvgAccumulator {
    sum: Option<f64>,
    count: Option<i64>,
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()> {
        match value {
            ColumnarValue::Columnar(array) => {
                // calculate sum
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
                // calculate average
                let avg = match sum {
                    Some(ScalarValue::Int8(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::Int16(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::Int32(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::Int64(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::UInt8(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::UInt16(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::UInt32(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::UInt64(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::Float32(n)) => {
                        Ok(Some(ScalarValue::Float64(n as f64 / array.len() as f64)))
                    }
                    Some(ScalarValue::Float64(n)) => {
                        Ok(Some(ScalarValue::Float64(n / array.len() as f64)))
                    }
                    _ => Err(ballista_error("tbd")),
                }?;
                self.accumulate(&ColumnarValue::Scalar(avg, 1))?;
            }
            ColumnarValue::Scalar(value, _) => {
                if let Some(value) = value {
                    match value {
                        ScalarValue::Int8(value) => avg_accumulate!(self, *value, Int8Array),
                        ScalarValue::Int16(value) => avg_accumulate!(self, *value, Int16Array),
                        ScalarValue::Int32(value) => avg_accumulate!(self, *value, Int32Array),
                        ScalarValue::Int64(value) => avg_accumulate!(self, *value, Int64Array),
                        ScalarValue::UInt8(value) => avg_accumulate!(self, *value, UInt8Array),
                        ScalarValue::UInt16(value) => avg_accumulate!(self, *value, UInt16Array),
                        ScalarValue::UInt32(value) => avg_accumulate!(self, *value, UInt32Array),
                        ScalarValue::UInt64(value) => avg_accumulate!(self, *value, UInt64Array),
                        ScalarValue::Float32(value) => avg_accumulate!(self, *value, Float32Array),
                        ScalarValue::Float64(value) => avg_accumulate!(self, *value, Float64Array),
                        other => {
                            return Err(ballista_error(&format!(
                                "AVG does not support {:?}",
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
        match (self.sum, self.count) {
            (Some(sum), Some(count)) => Ok(Some(ScalarValue::Float64(sum / count as f64))),
            _ => Ok(None),
        }
    }
}

/// Create an avg expression
pub fn avg(expr: Arc<dyn Expression>) -> Arc<dyn AggregateExpr> {
    Arc::new(Avg::new(expr))
}
