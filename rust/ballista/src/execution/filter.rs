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

use std::rc::Rc;

use async_trait::async_trait;

use crate::error::Result;
use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ExecutionPlan, PhysicalPlan,
};
use datafusion::logicalplan::Expr;
use tonic::codegen::Arc;

#[derive(Debug, Clone)]
pub struct FilterExec {
    pub(crate) child: Rc<PhysicalPlan>,
    filter_expr: Rc<Expr>,
}

impl ExecutionPlan for FilterExec {
    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        //TODO compile filter expr
        Ok(Arc::new(FilterIter {
            input: self.child.as_execution_plan().execute(partition_index)?,
            //filter_expr
        }))
    }
}

struct FilterIter {
    input: ColumnarBatchStream,
    //filter_expr: Arc<dyn Expression>
}

#[async_trait]
impl ColumnarBatchIter for FilterIter {
    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        Ok(self.input.next().await?.map(|batch| {
            //TODO apply filter expresion
            batch.clone()
        }))
    }
}
