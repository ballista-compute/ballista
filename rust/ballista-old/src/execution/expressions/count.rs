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

use crate::arrow::datatypes::{DataType, Schema};
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::Result;
use crate::execution::expressions::sum::SumAccumulator;
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, AggregateMode, ColumnarBatch, ColumnarValue, Expression,
};

#[derive(Debug)]
pub struct Count {
    input: Arc<dyn Expression>,
}

impl Count {
    pub fn new(input: Arc<dyn Expression>) -> Self {
        Self { input }
    }
}

impl AggregateExpr for Count {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue> {
        self.input.evaluate(batch)
    }

    fn create_accumulator(&self, mode: &AggregateMode) -> Box<dyn Accumulator> {
        match mode {
            AggregateMode::Partial => Box::new(CountAccumulator { count: 0 }),
            _ => Box::new(SumAccumulator { sum: None }),
        }
    }
}

struct CountAccumulator {
    count: u64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()> {
        match value {
            ColumnarValue::Columnar(array) => {
                self.count += array.len() as u64 - array.null_count() as u64;
            }
            ColumnarValue::Scalar(_, _) => {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn get_value(&self) -> Result<Option<ScalarValue>> {
        Ok(Some(ScalarValue::UInt64(self.count)))
    }
}

/// Create a count expression
pub fn count(expr: Arc<dyn Expression>) -> Arc<dyn AggregateExpr> {
    Arc::new(Count::new(expr))
}
