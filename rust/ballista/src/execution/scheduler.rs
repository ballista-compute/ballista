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
use crate::execution::shuffle_reader::ShuffleReaderExec;

/// A Job typically represents a single query and the query is executed in stages. Stages are
/// separated by map operations (shuffles) to re-partition data before the next stage starts.
#[derive(Debug)]
pub struct Job {
    /// Job UUID
    pub id: Uuid,
    /// A list of stages within this job. There can be dependencies between stages to form
    /// a directed acyclic graph (DAG).
    pub stages: Vec<Rc<RefCell<Stage>>>,
}

impl Job {
    pub fn explain(&self) {
        println!("Job {} has {} stages:\n", self.id, self.stages.len());
        self.stages.iter().for_each(|stage| {
            let stage = stage.as_ref().borrow();
            println!("Stage {}:\n", stage.id);
            if stage.prior_stages.is_empty() {
                println!("Stage {} has no dependencies.", stage.id);
            } else {
                println!(
                    "Stage {} depends on stages {:?}.",
                    stage.id, stage.prior_stages
                );
            }
            println!(
                "\n{:?}\n",
                stage
                    .plan
                    .as_ref()
                    .expect("Stages should always have a plan")
            );
        })
    }
}

/// A query stage consists of tasks. Typically, tasks map to partitions.
#[derive(Debug)]
pub struct Stage {
    /// Stage id which is unique within a job.
    pub id: usize,
    /// A list of stages that must complete before this stage can execute.
    pub prior_stages: Vec<usize>,
    /// The physical plan to execute for this stage
    pub plan: Option<Rc<PhysicalPlan>>,
}

impl Stage {
    /// Create a new empty stage with the specified id.
    fn new(id: usize) -> Self {
        Self {
            id,
            prior_stages: vec![],
            plan: None,
        }
    }
}

/// Create a Job (DAG of stages) from a physical execution plan.
pub fn create_job(plan: Rc<PhysicalPlan>) -> Result<Job> {
    let mut scheduler = Scheduler::new();
    scheduler.create_job(plan)?;
    Ok(scheduler.job)
}

pub struct Scheduler {
    job: Job,
    next_stage_id: usize,
}

impl Scheduler {
    fn new() -> Self {
        let job = Job {
            id: Uuid::new_v4(),
            stages: vec![],
        };
        Self {
            job,
            next_stage_id: 0,
        }
    }

    fn create_job(&mut self, plan: Rc<PhysicalPlan>) -> Result<()> {
        let new_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let new_stage = Rc::new(RefCell::new(Stage::new(new_stage_id)));
        self.job.stages.push(new_stage.clone());
        let plan = self.visit_plan(plan, new_stage.clone())?;
        new_stage.as_ref().borrow_mut().plan = Some(plan);
        Ok(())
    }

    fn visit_plan(
        &mut self,
        plan: Rc<PhysicalPlan>,
        current_stage: Rc<RefCell<Stage>>,
    ) -> Result<Rc<PhysicalPlan>> {
        //
        match plan.as_ref() {
            PhysicalPlan::ShuffleExchange(exec) => {
                // shuffle indicates that we need a new stage
                let new_stage_id = self.next_stage_id;
                self.next_stage_id += 1;
                let new_stage = Rc::new(RefCell::new(Stage::new(new_stage_id)));
                self.job.stages.push(new_stage.clone());

                // the children need to be part of this new stage
                let shuffle_input = self.visit_plan(exec.child.clone(), new_stage.clone())?;

                new_stage.as_ref().borrow_mut().plan = Some(shuffle_input);

                // the current stage depends on this new stage
                current_stage
                    .as_ref()
                    .borrow_mut()
                    .prior_stages
                    .push(new_stage_id);

                // return a shuffle reader to read the results from the stage
                Ok(Rc::new(PhysicalPlan::ShuffleReader(Rc::new(
                    ShuffleReaderExec {
                        stage_id: new_stage_id,
                    },
                ))))
            }
            PhysicalPlan::HashAggregate(exec) => {
                let child = self.visit_plan(exec.child.clone(), current_stage)?;
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    exec.with_new_children(vec![child]),
                ))))
            }
            PhysicalPlan::ParquetScan(_) => Ok(plan.clone()),
            _ => unimplemented!(),
        }
    }
}

/// Convert a logical plan into a physical plan
pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Rc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, expr, .. } => {
            let exec = ProjectionExec::try_new(expr, create_physical_plan(input)?)?;
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
                let exec = HashAggregateExec::try_new(
                    AggregateMode::Final,
                    group_expr.clone(),
                    aggr_expr.clone(),
                    input,
                )?;
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(exec))))
            } else {
                // Create partial hash aggregate to run against partitions in parallel
                let partial_hash_exec = HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    group_expr.clone(),
                    aggr_expr.clone(),
                    input,
                )?;
                let partial = Rc::new(PhysicalPlan::HashAggregate(Rc::new(partial_hash_exec)));
                // Create final hash aggregate to run on the coalesced partition of the results
                // from the partial hash aggregate
                // TODO these are not the correct expressions being passed in here for the final agg
                let final_hash_exec = HashAggregateExec::try_new(
                    AggregateMode::Final,
                    group_expr.clone(),
                    aggr_expr.clone(),
                    partial,
                )?;
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    final_hash_exec,
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::dataframe::{col, sum, Context};
//     use std::collections::HashMap;
//
//     #[test]
//     fn create_plan() -> Result<()> {
//         let ctx = Context::local(HashMap::new());
//
//         let df = ctx
//             .read_parquet("/mnt/nyctaxi/parquet/year=2019", None)?
//             .aggregate(vec![col("passenger_count")], vec![sum(col("fare_amount"))])?;
//
//         let plan = df.logical_plan();
//
//         let plan = create_physical_plan(&plan)?;
//         let plan = ensure_requirements(&plan)?;
//         println!("Optimized physical plan:\n{:?}", plan);
//
//         let mut scheduler = Scheduler::new();
//         scheduler.create_job(plan)?;
//
//         scheduler.job.explain();
//         Ok(())
//     }
// }
