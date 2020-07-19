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
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};

#[derive(Debug)]
pub struct Add {
    l: Arc<dyn Expression>,
    r: Arc<dyn Expression>,
}

impl Add {
    pub fn new(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Self {
        Self { l, r }
    }
}

pub fn add(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Arc<dyn Expression> {
    Arc::new(Add::new(l, r))
}

impl Expression for Add {
    fn name(&self) -> String {
        format!("{} + {}", self.l.name(), self.r.name())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.l.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow();
        let r = self.r.evaluate(input)?.to_arrow();
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Add expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int32 => {
                let l = cast_array!(l, Int32Array)?;
                let r = cast_array!(r, Int32Array)?;
                Ok(ColumnarValue::Columnar(Arc::new(compute::add(l, r)?)))
            }
            _ => Err(ballista_error("Unsupported datatype for Add expression")),
        }
    }
}
