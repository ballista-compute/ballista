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

use crate::arrow::datatypes::Schema;
use crate::error::Result;
use crate::execution::physical_plan::{
    ColumnarBatchStream, ExecutionContext, ExecutionPlan, Partitioning, PhysicalPlan,
};

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ShuffleExchangeExec {
    pub(crate) child: Arc<PhysicalPlan>,
    output_partitioning: Partitioning,
}

impl ShuffleExchangeExec {
    pub fn new(child: Arc<PhysicalPlan>, output_partitioning: Partitioning) -> Self {
        Self {
            child,
            output_partitioning,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleExchangeExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.as_execution_plan().schema()
    }

    async fn execute(
        &self,
        _ctx: Arc<dyn ExecutionContext>,
        _partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        unimplemented!()
    }
}
