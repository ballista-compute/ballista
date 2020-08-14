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
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};

#[derive(Debug)]
pub struct Literal {
    value: ScalarValue,
}

impl Literal {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl Expression for Literal {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.get_datatype()?)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone(), input.num_rows()))
    }
}

pub fn lit(value: ScalarValue) -> Arc<dyn Expression> {
    Arc::new(Literal::new(value))
}
