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

use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};
use arrow::datatypes::{DataType, Schema};

/// Reference to a column by index
#[derive(Debug)]
pub struct ColumnReference {
    index: usize,
    name: String,
}

impl ColumnReference {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            name: format!("c{}", index),
        }
    }
}

impl Expression for ColumnReference {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema.field(self.index).data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(input_schema.field(self.index).is_nullable())
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        Ok(input.column(self.index).clone())
    }
}
