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
use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue, ExecutionPlan,
    Expression, Partitioning, PhysicalPlan,
};
use async_trait::async_trait;
use std::rc::Rc;
use tonic::codegen::Arc;

#[derive(Debug, Clone)]
pub struct ProjectionExec {
    pub(crate) child: Rc<PhysicalPlan>,
}

impl ProjectionExec {
    pub fn new(child: Rc<PhysicalPlan>) -> Self {
        Self { child }
    }
}

impl ExecutionPlan for ProjectionExec {
    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.as_execution_plan().output_partitioning()
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        // TODO compile expressions
        Ok(Arc::new(ProjectionIter {
            input: self.child.as_execution_plan().execute(partition_index)?,
            projection: vec![], // TODO
        }))
    }
}

/// Iterator that applies a projection to the batches
struct ProjectionIter {
    input: ColumnarBatchStream,
    projection: Vec<Arc<dyn Expression>>,
}

#[async_trait]
impl ColumnarBatchIter for ProjectionIter {
    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        Ok(self.input.next().await?.map(|batch| {
            let projected_values: Vec<ColumnarValue> =
                self.projection.iter().map(|e| e.evaluate(&batch)).collect();
            ColumnarBatch::from_values(&projected_values)
        }))
    }
}
