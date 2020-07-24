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
use crate::error::Result;
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, AggregateMode, ColumnarBatch, ColumnarValue, Expression,
};

#[derive(Debug)]
pub struct Alias {
    expr: Arc<dyn Expression>,
    alias: String,
}

impl Alias {
    pub fn new(expr: Arc<dyn Expression>, alias: &str) -> Self {
        Self {
            expr,
            alias: alias.to_owned(),
        }
    }
}

impl Expression for Alias {
    fn name(&self) -> String {
        self.alias.clone()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        self.expr.evaluate(input)
    }
}

pub fn alias(expr: Arc<dyn Expression>, alias: &str) -> Arc<dyn Expression> {
    Arc::new(Alias::new(expr, alias))
}

#[derive(Debug)]
pub struct AliasedAggregate {
    expr: Arc<dyn AggregateExpr>,
    alias: String,
}

impl AliasedAggregate {
    pub fn new(expr: Arc<dyn AggregateExpr>, alias: &str) -> Self {
        Self {
            expr,
            alias: alias.to_owned(),
        }
    }
}

impl AggregateExpr for AliasedAggregate {
    fn name(&self) -> String {
        self.alias.clone()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue> {
        self.expr.evaluate_input(batch)
    }

    fn create_accumulator(&self, mode: &AggregateMode) -> Box<dyn Accumulator> {
        self.expr.create_accumulator(mode)
    }
}

pub fn aliased_aggr(expr: Arc<dyn AggregateExpr>, alias: &str) -> Arc<dyn AggregateExpr> {
    Arc::new(AliasedAggregate::new(expr, alias))
}
