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

//! Column reference by index.

use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Schema};
use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};

/// Reference to a column by name
#[derive(Debug)]
pub struct Column {
    name: String,
}

impl Column {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

pub fn col(name: &str) -> Arc<dyn Expression> {
    Arc::new(Column::new(name))
}

impl Expression for Column {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema.field_with_name(&self.name)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(input_schema.field_with_name(&self.name)?.is_nullable())
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        Ok(input.column(&self.name)?.clone())
    }
}
