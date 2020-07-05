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

//! The scheduler is responsible for breaking a physical query plan down into stages and tasks
//! and co-ordinating execution of these stages and tasks across the cluster.

use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;

use crate::datafusion::logicalplan::LogicalPlan;
use crate::error::{BallistaError, Result};
use crate::execution::hash_aggregate::HashAggregateExec;
use crate::execution::parquet_scan::ParquetScanExec;
use crate::execution::physical_plan::{AggregateMode, Distribution, Partitioning, PhysicalPlan};
use crate::execution::projection::ProjectionExec;
use crate::execution::shuffle_exchange::ShuffleExchangeExec;

/// A Job typically represents a single query and the query is executed in stages. Stages are
/// separated by map operations (shuffles) to re-partition data before the next stage starts.
pub struct Job {
    /// Job UUID
    pub id: Uuid,
    /// A list of stages within this job. There can be dependencies between stages to form
    /// a directed acyclic graph (DAG).
    pub stages: Vec<Rc<Stage>>,
}

/// A query stage consists of tasks. Typically, tasks map to partitions.
pub struct Stage {
    /// Stage id which is unique within a job.
    pub id: usize,
    /// A list of stages that must complete before this stage can execute.
    pub prior_stages: Vec<usize>,
    /// A list of tasks in this stage. One task per partition currently.
    pub tasks: Vec<Task>,
}

impl Stage {
    /// Create a new empty stage with the specified id.
    fn new(id: usize) -> Self {
        Self {
            id,
            prior_stages: vec![],
            tasks: vec![]
        }
    }
}

/// A Task represents a physical query plan to be executed against a partition (and later, possibly
/// against partition splits).
pub struct Task {
    /// Task id which is unique within a stage.
    pub id: usize,
    /// The partition that this task will execute against
    pub partition_id: usize,
    /// The split or chunk of the partition. Currently unused.
    pub split_id: usize,
    /// The physical plan to execute
    pub plan: Rc<PhysicalPlan>,
}

impl Task {
    pub fn new(id: usize, partition_id: usize, split_id: usize, plan: Rc<PhysicalPlan>) -> Self {
        Self { id, partition_id, split_id, plan }
    }
}

pub struct Scheduler {
    current_stage: Option<Rc<RefCell<Stage>>>,
    next_stage_id: usize,
    next_task_id: usize,
}

impl Scheduler {

    fn new() -> Self {
        Self {
            current_stage: Some(Rc::new(RefCell::new(Stage::new(0)))),
            next_stage_id: 1,
            next_task_id: 0,
        }
    }

    fn create_dag(&mut self, plan: Rc<PhysicalPlan>) {
        match plan.as_ref() {
            PhysicalPlan::HashAggregate(exec) => {
                self.create_dag(exec.child.clone());
            }
            PhysicalPlan::ShuffleExchange(exec) => {
                self.create_dag(exec.child.clone());
            }
            PhysicalPlan::ParquetScan(exec) => {
                let mut current_stage = self.current_stage.as_ref()
                    .expect("stage must exist when creating ParquetScan tasks")
                    .borrow_mut();

                // create one task per file for now but later we may want finer granularity of one
                // task per chunk or split.
                for (i, _) in exec.filenames.iter().enumerate() {
                    current_stage.tasks.push(Task::new(self.next_task_id, i, 0, plan.clone()));
                    self.next_task_id += 1;

                }
                    //.for_each(|(i, filename)| current_stage.add_task(i, plan.clone()));
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Rc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, .. } => {
            let exec = ProjectionExec::new(create_physical_plan(input)?);
            Ok(Rc::new(PhysicalPlan::Projection(Rc::new(exec))))
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } => {
            let input = create_physical_plan(input)?;
            if input
                .as_execution_plan()
                .output_partitioning()
                .partition_count()
                == 1
            {
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Final,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        input,
                    ),
                ))))
            } else {
                let partial = Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Partial,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        input,
                    ),
                )));
                // TODO these are not the correct expressions being passed in here for the final agg
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Final,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        partial,
                    ),
                ))))
            }
        }
        LogicalPlan::ParquetScan { path, .. } => {
            let exec = ParquetScanExec::try_new(&path, None)?;
            Ok(Rc::new(PhysicalPlan::ParquetScan(Rc::new(exec))))
        }
        other => Err(BallistaError::General(format!("unsupported {:?}", other))),
    }
}

/// Optimizer rule to insert shuffles as needed
pub fn ensure_requirements(plan: &PhysicalPlan) -> Result<Rc<PhysicalPlan>> {
    let execution_plan = plan.as_execution_plan();

    // recurse down and replace children
    if execution_plan.children().is_empty() {
        return Ok(Rc::new(plan.clone()));
    }
    let children: Vec<Rc<PhysicalPlan>> = execution_plan
        .children()
        .iter()
        .map(|c| ensure_requirements(c.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    match execution_plan.required_child_distribution() {
        Distribution::SinglePartition => {
            let new_children: Vec<Rc<PhysicalPlan>> = children
                .iter()
                .map(|c| {
                    if c.as_execution_plan()
                        .output_partitioning()
                        .partition_count()
                        > 1
                    {
                        Rc::new(PhysicalPlan::ShuffleExchange(Rc::new(
                            ShuffleExchangeExec::new(
                                c.clone(),
                                Partitioning::UnknownPartitioning(1),
                            ),
                        )))
                    } else {
                        Rc::new(plan.clone())
                    }
                })
                .collect();

            Ok(Rc::new(plan.with_new_children(new_children)))
        }
        _ => Ok(Rc::new(plan.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{col, sum, Context};
    use std::collections::HashMap;

    #[test]
    fn create_plan() -> Result<()> {
        let ctx = Context::local(HashMap::new());

        let df = ctx
            .read_parquet("/mnt/nyctaxi/parquet/year=2019", None)?
            .aggregate(vec![col("passenger_count")], vec![sum(col("fare_amount"))])?;

        let plan = df.logical_plan();

        let plan = create_physical_plan(&plan)?;
        let plan = ensure_requirements(&plan)?;
        println!("{:?}", plan);

        let mut scheduler = Scheduler::new();
        scheduler.create_dag(plan);

        Ok(())
    }
}
