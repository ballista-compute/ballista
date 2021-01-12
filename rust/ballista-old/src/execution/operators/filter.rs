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

//! Filter operator.

use std::sync::Arc;

use crate::arrow;
use crate::arrow::array;
use crate::arrow::datatypes::Schema;
use crate::datafusion::logicalplan::Expr;
use crate::error::{ballista_error, Result};
use crate::{
    cast_array,
    execution::physical_plan::{
        compile_expression, ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue,
        ExecutionContext, ExecutionPlan, Expression, PhysicalPlan,
    },
};

use crate::execution::physical_plan::Partitioning;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

/// FilterExec evaluates a boolean expression against each row of input to determine which rows
/// to include in output batches.
#[derive(Debug, Clone)]
pub struct FilterExec {
    pub(crate) child: Arc<PhysicalPlan>,
    pub(crate) filter_expr: Arc<Expr>,
}

impl FilterExec {
    pub fn new(child: &PhysicalPlan, filter_expr: &Expr) -> Self {
        Self {
            child: Arc::new(child.clone()),
            filter_expr: Arc::new(filter_expr.clone()),
        }
    }
}

impl FilterExec {
    pub fn with_new_children(&self, new_children: Vec<Arc<PhysicalPlan>>) -> FilterExec {
        assert!(new_children.len() == 1);
        FilterExec {
            filter_expr: self.filter_expr.clone(),
            child: new_children[0].clone(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for FilterExec {
    fn schema(&self) -> Arc<Schema> {
        // a filter does not alter the schema
        self.child.as_execution_plan().schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.as_execution_plan().output_partitioning()
    }

    fn children(&self) -> Vec<Arc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    async fn execute(
        &self,
        ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        let expr = compile_expression(&self.filter_expr, &self.schema())?;
        Ok(Arc::new(FilterIter {
            input: self
                .child
                .as_execution_plan()
                .execute(ctx.clone(), partition_index)
                .await?,
            filter_expr: expr,
        }))
    }
}

#[allow(dead_code)]
struct FilterIter {
    input: ColumnarBatchStream,
    filter_expr: Arc<dyn Expression>,
}

impl ColumnarBatchIter for FilterIter {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn next(&self) -> Result<Option<ColumnarBatch>> {
        match self.input.next()? {
            Some(input) => {
                let bools = self.filter_expr.evaluate(&input)?;
                let batch = apply_filter(&input, &bools)?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
}

/// Filter the provided batch based on the bitmask
fn apply_filter(batch: &ColumnarBatch, bitmask: &ColumnarValue) -> Result<ColumnarBatch> {
    let predicate = bitmask.to_arrow()?;
    let predicate = cast_array!(predicate, BooleanArray)?;

    let mut filtered_arrays = Vec::with_capacity(batch.num_columns());
    for column in batch.schema().fields().iter().map(|f| f.name()) {
        let array = batch.column(column)?;
        let filtered_array = arrow::compute::filter(array.to_arrow()?.as_ref(), predicate)?;
        filtered_arrays.push(filtered_array);
    }
    Ok(ColumnarBatch::from_arrow(&RecordBatch::try_new(
        batch.schema(),
        filtered_arrays,
    )?))
}
