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

//! Distributed query execution
//!
//! This code is EXPERIMENTAL and still under development

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::client::BallistaClient;
use crate::context::DFTableAdapter;
use crate::error::Result;
use crate::executor::query_stage::QueryStageExec;
use crate::executor::shuffle_reader::ShuffleReaderExec;
use crate::serde::scheduler::ExecutorMeta;
use crate::serde::scheduler::PartitionId;

use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub(crate) partition_id: PartitionId,
    pub(crate) executor_meta: ExecutorMeta,
}

/// Trait that the distributed planner uses to get a list of available executors
pub trait SchedulerClient: Send + Sync {
    fn get_executors(&self) -> Result<Vec<ExecutorMeta>>;
}

impl SchedulerClient for Vec<ExecutorMeta> {
    fn get_executors(&self) -> Result<Vec<ExecutorMeta>> {
        Ok(self.clone())
    }
}

pub struct DistributedPlanner {
    scheduler_client: Box<dyn SchedulerClient>,
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn new(scheduler_client: Box<dyn SchedulerClient>) -> Self {
        Self {
            scheduler_client,
            next_stage_id: 0,
        }
    }
}

impl DistributedPlanner {
    pub async fn execute_distributed_query(
        &mut self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let job_uuid = Uuid::new_v4();

        let execution_plan = self.prepare_query_stages(&job_uuid, execution_plan)?;

        // wrap final operator in query stage
        let execution_plan =
            create_query_stage(&job_uuid, self.next_stage_id(), execution_plan.clone())?;
        pretty_print(execution_plan.clone(), 0);

        let executors = self.scheduler_client.get_executors()?;

        execute(execution_plan.clone(), executors.clone()).await
    }

    /// Insert QueryStageExec nodes into the plan wherever partitioning changes
    pub fn prepare_query_stages(
        &mut self,
        job_uuid: &Uuid,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok(execution_plan.clone());
        }

        let children: Vec<Arc<dyn ExecutionPlan>> = execution_plan
            .children()
            .iter()
            .map(|c| self.prepare_query_stages(&job_uuid, c.clone()))
            .collect::<Result<Vec<_>>>()?;

        if let Some(adapter) = execution_plan.as_any().downcast_ref::<DFTableAdapter>() {
            let ctx = ExecutionContext::new();
            Ok(ctx.create_physical_plan(&adapter.logical_plan)?)
        } else if let Some(merge) = execution_plan.as_any().downcast_ref::<MergeExec>() {
            let child = merge.children()[0].clone();
            Ok(Arc::new(QueryStageExec::try_new(
                *job_uuid,
                self.next_stage_id(),
                child,
            )?))
        } else if let Some(agg) = execution_plan.as_any().downcast_ref::<HashAggregateExec>() {
            //TODO should insert query stages in more generic way based on partitioning metadata
            // and not specifically for this operator
            match agg.mode() {
                AggregateMode::Final => {
                    let children = children
                        .iter()
                        .map(|plan| {
                            create_query_stage(job_uuid, self.next_stage_id(), plan.clone())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(agg.with_new_children(children)?)
                }
                AggregateMode::Partial => Ok(agg.with_new_children(children)?),
            }
        } else if let Some(join) = execution_plan.as_any().downcast_ref::<HashJoinExec>() {
            Ok(join.with_new_children(vec![
                create_query_stage(&*job_uuid, self.next_stage_id(), join.left().clone())?,
                create_query_stage(&*job_uuid, self.next_stage_id(), join.right().clone())?,
            ])?)
        } else {
            // TODO check for compatible partitioning schema, not just count
            if execution_plan.output_partitioning().partition_count()
                != children[0].output_partitioning().partition_count()
            {
                let children = children
                    .iter()
                    .map(|plan| create_query_stage(job_uuid, self.next_stage_id(), plan.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(execution_plan.with_new_children(children)?)
            } else {
                Ok(execution_plan.with_new_children(children)?)
            }
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

/// Visitor pattern to walk the plan, depth-first, and then execute query stages when walking
/// up the tree
fn execute(
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + Sync>> {
    Box::pin(async move {
        debug!("execute() {}", &format!("{:?}", plan)[0..60]);
        let executors = executors.to_vec();
        // execute children first
        let mut children: Vec<Arc<dyn ExecutionPlan>> = vec![];
        for child in plan.children() {
            let executed_child = execute(child.clone(), executors.clone()).await?;
            children.push(executed_child);
        }
        let plan = plan.with_new_children(children)?;

        let new_plan: Arc<dyn ExecutionPlan> = if plan.as_any().is::<QueryStageExec>() {
            let stage = plan
                .clone()
                .as_any()
                .downcast_ref::<QueryStageExec>()
                .unwrap();
            let partition_locations = execute_query_stage(
                &stage.job_uuid.clone(),
                stage.stage_id,
                stage.children()[0].clone(),
                executors.clone(),
            )
            .await?;

            // replace the query stage with a ShuffleReaderExec that can read the partitions
            // produced by the executed query stage
            let shuffle_reader = ShuffleReaderExec::try_new(partition_locations, stage.schema())?;
            Arc::new(shuffle_reader)
        } else {
            plan
        };

        debug!("execute is returning:");
        pretty_print(new_plan.clone(), 0);

        Ok(new_plan)
    })
}

// struct Foo {
//     plan: Arc<dyn ExecutionPlan>,
//     partition_locations: Vec<PartitionLocation>
// }

fn create_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(QueryStageExec::try_new(
        *job_uuid,
        stage_id,
        plan.clone(),
    )?))
}

/// Execute a query stage by sending each partition to an executor
async fn execute_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Result<Vec<PartitionLocation>> {
    debug!("execute_query_stage() stage_id={}", stage_id);
    pretty_print(plan.clone(), 0);

    let partition_count = plan.output_partitioning().partition_count();
    let mut meta = Vec::with_capacity(partition_count);

    // TODO make this concurrent by executing all partitions at once instead of one at a time

    for child_partition in 0..partition_count {
        let executor_meta = &executors[child_partition % executors.len()];
        let mut client = BallistaClient::try_new(&executor_meta.host, executor_meta.port as usize)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        let _partition_metadata = client
            .execute_partition(*job_uuid, stage_id, child_partition, plan.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        meta.push(PartitionLocation {
            partition_id: PartitionId::new(*job_uuid, stage_id, child_partition),
            executor_meta: executor_meta.clone(),
        });
    }

    debug!(
        "execute_query_stage() stage_id={} produced {:?}",
        stage_id, meta
    );

    Ok(meta)
}

pub fn pretty_print(plan: Arc<dyn ExecutionPlan>, indent: usize) {
    for _ in 0..indent {
        print!("  ");
    }
    let operator_str = format!("{:?}", plan);
    println!("{}", &operator_str[0..60]);
    plan.children()
        .iter()
        .for_each(|c| pretty_print(c.clone(), indent + 1));
}
