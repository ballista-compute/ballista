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

//! Projection operator.

use std::sync::Arc;

use crate::arrow::datatypes::Schema;
use crate::datafusion::logicalplan::Expr;
use crate::error::Result;
use crate::execution::physical_plan::{
    compile_expressions, ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue,
    ExecutionContext, ExecutionPlan, Expression, Partitioning, PhysicalPlan,
};

use async_trait::async_trait;

/// Projection operator evaluates expressions against an input.
#[derive(Debug, Clone)]
pub struct ProjectionExec {
    /// Logical expressions for the projection.
    pub(crate) exprs: Vec<Expr>,
    /// The input operator to apply the projection to.
    pub(crate) child: Arc<PhysicalPlan>,
    /// The resulting schema of the projection.
    pub(crate) schema: Arc<Schema>,
}

impl ProjectionExec {
    pub fn try_new(exprs: &[Expr], child: Arc<PhysicalPlan>) -> Result<Self> {
        let input_schema = child.as_execution_plan().schema();
        let pexprs = compile_expressions(&exprs, &child.as_execution_plan().schema())?;
        let fields: Result<Vec<_>> = pexprs
            .iter()
            .map(|e| e.to_schema_field(&input_schema))
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            exprs: exprs.to_vec(),
            child,
            schema,
        })
    }

    pub fn with_new_children(&self, new_children: Vec<Arc<PhysicalPlan>>) -> ProjectionExec {
        assert!(new_children.len() == 1);
        ProjectionExec {
            exprs: self.exprs.clone(),
            child: new_children[0].clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ProjectionExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.as_execution_plan().output_partitioning()
    }

    async fn execute(
        &self,
        ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        let input = &self.child.as_execution_plan();
        let projection = compile_expressions(&self.exprs, &input.schema())?;
        Ok(Arc::new(ProjectionIter {
            input: input.execute(ctx.clone(), partition_index).await?,
            projection,
            schema: self.schema.clone(),
        }))
    }
}

/// Iterator that applies a projection to the batches
struct ProjectionIter {
    input: ColumnarBatchStream,
    projection: Vec<Arc<dyn Expression>>,
    schema: Arc<Schema>,
}

#[async_trait]
impl ColumnarBatchIter for ProjectionIter {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        match self.input.next().await? {
            Some(batch) => {
                let projected_values: Vec<ColumnarValue> = self
                    .projection
                    .iter()
                    .map(|e| e.evaluate(&batch))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(ColumnarBatch::from_values_and_schema(
                    &projected_values,
                    self.schema.clone(),
                )))
            }
            None => Ok(None),
        }
    }
}
