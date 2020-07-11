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

use async_trait::async_trait;

use crate::error::Result;
use crate::execution::physical_plan::{
    compile_expression, ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue,
    ExecutionPlan, Expression, PhysicalPlan,
};
use arrow::datatypes::Schema;
use datafusion::logicalplan::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FilterExec {
    pub(crate) child: Arc<PhysicalPlan>,
    filter_expr: Arc<Expr>,
}

impl ExecutionPlan for FilterExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn children(&self) -> Vec<Arc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        //TODO compile filter expr
        let expr = compile_expression(&self.filter_expr, &self.schema())?;
        Ok(Arc::new(FilterIter {
            input: self.child.as_execution_plan().execute(partition_index)?,
            filter_expr: expr,
        }))
    }
}

#[allow(dead_code)]
struct FilterIter {
    input: ColumnarBatchStream,
    filter_expr: Arc<dyn Expression>,
}

#[async_trait]
impl ColumnarBatchIter for FilterIter {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        let _input = self.input.next().await?;
        unimplemented!()
    }
}

#[allow(dead_code)]
fn apply_filter(_batch: &ColumnarBatch, _filter_bools: &ColumnarValue) -> Result<ColumnarBatch> {
    unimplemented!()
}
