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

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::execution::context::ExecutionContext;
use crate::datafusion::logicalplan::LogicalPlan;
use crate::error::Result;
use crate::execution::physical_plan::ShuffleId;

use async_trait::async_trait;

pub struct ShufflePartition {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<RecordBatch>,
}

#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute a query and store the resulting shuffle partitions in memory
    async fn do_task(&self) -> ShuffleId;

    /// Execute a query and return results
    async fn execute_query(&self, plan: &LogicalPlan) -> Result<ShufflePartition>;
}

pub struct BallistaExecutor {}

#[allow(clippy::new_without_default)]
impl BallistaExecutor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Executor for BallistaExecutor {
    async fn do_task(&self) -> ShuffleId {
        unimplemented!()
    }

    async fn execute_query(&self, logical_plan: &LogicalPlan) -> Result<ShufflePartition> {
        // note that this used DataFusion and not the new Ballista async / distributed
        // query execution

        // create local execution context
        let ctx = ExecutionContext::new();

        // create the query plan
        let optimized_plan = ctx.optimize(&logical_plan)?;

        let batch_size = 1024 * 1024;
        let physical_plan = ctx.create_physical_plan(&optimized_plan, batch_size)?;

        // execute the query
        let results = ctx.collect(physical_plan.as_ref())?;

        let schema = physical_plan.schema();

        Ok(ShufflePartition {
            schema: schema.as_ref().clone(),
            data: results,
        })
    }
}
