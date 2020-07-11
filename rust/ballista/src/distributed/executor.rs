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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::execution::context::ExecutionContext as DFContext;
use crate::datafusion::logicalplan::LogicalPlan;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{
    Action, ColumnarBatch, ExecutionContext, ShuffleId, ShuffleManager,
};
use crate::execution::scheduler::ExecutionTask;

use crate::distributed::client::execute_action;
use async_trait::async_trait;

#[derive(Clone)]
pub struct ShufflePartition {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<RecordBatch>,
}

#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute a query and store the resulting shuffle partitions in memory
    async fn do_task(&self, task: &ExecutionTask) -> Result<ShuffleId>;

    /// Collect the results of a prior task that resulted in a shuffle partition
    fn collect(&self, shuffle_id: &ShuffleId) -> Result<ShufflePartition>;

    /// Execute a query and return results
    async fn execute_query(&self, plan: &LogicalPlan) -> Result<ShufflePartition>;
}

pub struct ExecutorContext {}

impl ExecutionContext for ExecutorContext {
    fn shuffle_manager(&self) -> Arc<dyn ShuffleManager> {
        Arc::new(DefaultShuffleManager {})
    }
}

pub struct DefaultShuffleManager {}

#[async_trait]
impl ShuffleManager for DefaultShuffleManager {
    async fn read_shuffle(&self, shuffle_id: &ShuffleId) -> Result<Vec<ColumnarBatch>> {
        // TODO etcd lookup to find executor
        let batches =
            execute_action("localhost", 50051, Action::FetchShuffle(shuffle_id.clone())).await?;
        Ok(batches
            .iter()
            .map(|b| ColumnarBatch::from_arrow(b))
            .collect())
    }
}

pub struct BallistaExecutor {
    ctx: Arc<dyn ExecutionContext>,
    shuffle_partitions: Arc<Mutex<HashMap<String, ShufflePartition>>>,
}

impl BallistaExecutor {
    pub fn new(ctx: Arc<dyn ExecutionContext>) -> Self {
        Self {
            ctx,
            shuffle_partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for BallistaExecutor {
    fn default() -> Self {
        BallistaExecutor::new(Arc::new(ExecutorContext {}))
    }
}

#[async_trait]
impl Executor for BallistaExecutor {
    async fn do_task(&self, task: &ExecutionTask) -> Result<ShuffleId> {
        let shuffle_id = ShuffleId::new(task.job_uuid, task.stage_id, task.partition_id);

        let exec_plan = task.plan.as_execution_plan();
        let stream = exec_plan
            .execute(self.ctx.clone(), task.partition_id)
            .await?;
        let mut batches = vec![];
        while let Some(batch) = stream.next().await? {
            batches.push(batch.to_arrow()?);
        }

        let key = format!(
            "{}:{}:{}",
            shuffle_id.job_uuid, shuffle_id.stage_id, shuffle_id.partition_id
        );
        let mut shuffle_partitions = self.shuffle_partitions.lock().unwrap();
        shuffle_partitions.insert(
            key,
            ShufflePartition {
                schema: stream.schema().as_ref().clone(),
                data: batches,
            },
        );

        Ok(shuffle_id)
    }

    fn collect(&self, shuffle_id: &ShuffleId) -> Result<ShufflePartition> {
        let key = format!(
            "{}:{}:{}",
            shuffle_id.job_uuid, shuffle_id.stage_id, shuffle_id.partition_id
        );
        let shuffle_partitions = self.shuffle_partitions.lock().unwrap();
        match shuffle_partitions.get(&key) {
            Some(partition) => Ok(partition.clone()),
            _ => Err(ballista_error("invalid shuffle partition id")),
        }
    }

    async fn execute_query(&self, logical_plan: &LogicalPlan) -> Result<ShufflePartition> {
        // note that this used DataFusion and not the new Ballista async / distributed
        // query execution

        // create local execution context
        let ctx = DFContext::new();

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
