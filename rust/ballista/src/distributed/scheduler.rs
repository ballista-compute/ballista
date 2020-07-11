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
use std::sync::Arc;
use uuid::Uuid;

use crate::datafusion::logicalplan::LogicalPlan;
use crate::distributed::executor::Executor;
use crate::error::{ballista_error, BallistaError, Result};
use crate::execution::operators::HashAggregateExec;
use crate::execution::operators::ParquetScanExec;
use crate::execution::operators::ProjectionExec;
use crate::execution::operators::ShuffleExchangeExec;
use crate::execution::operators::ShuffleReaderExec;
use crate::execution::physical_plan::{
    AggregateMode, ColumnarBatch, Distribution, Partitioning, PhysicalPlan,
};
use std::collections::HashMap;
use crate::execution::shuffle_manager::ShuffleManager;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Execute the query and return the results
    Collect { plan: LogicalPlan },
    /// Execute a task within a query stage
    ExecuteTask { task: Task },
    /// Execute the query and write the results to CSV
    WriteCsv { plan: LogicalPlan, path: String },
    /// Execute the query and write the results to Parquet
    WriteParquet { plan: LogicalPlan, path: String },
}

/// A Job typically represents a single query and the query is executed in stages. Stages are
/// separated by map operations (shuffles) to re-partition data before the next stage starts.
#[derive(Debug)]
pub struct Job {
    /// Job UUID
    pub id: Uuid,
    /// A list of stages within this job. There can be dependencies between stages to form
    /// a directed acyclic graph (DAG).
    pub stages: Vec<Rc<RefCell<Stage>>>,
    /// The root stage id that produces the final results
    pub root_stage_id: usize,
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
    pub plan: Option<Arc<PhysicalPlan>>,
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

/// Task that can be sent to an executor for execution
#[derive(Debug, Clone)]
pub struct Task {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) task_id: usize,
    pub(crate) partition_id: usize,
    pub(crate) plan: PhysicalPlan,
}

impl Task {
    pub fn new(
        job_uuid: Uuid,
        stage_id: usize,
        task_id: usize,
        partition_id: usize,
        plan: &PhysicalPlan,
    ) -> Self {
        Self {
            job_uuid,
            stage_id,
            task_id,
            partition_id,
            plan: plan.clone(),
        }
    }

    /// Create a string that can be used as a key in a hash map to uniquely identify state
    /// associated with this task
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.job_uuid.to_string(),
            self.stage_id,
            self.task_id
        )
    }
}

/// Create a Job (DAG of stages) from a physical execution plan.
pub fn create_job(plan: Arc<PhysicalPlan>) -> Result<Job> {
    let mut scheduler = Scheduler::new();
    scheduler.create_job(plan)?;
    Ok(scheduler.job)
}

enum StageStatus {
    Pending,
    Completed,
}

/// Execute a job directly against executors as starting point
pub async fn execute_job(
    job: &Job,
    executors: Vec<Arc<dyn Executor>>,
) -> Result<Vec<ColumnarBatch>> {
    let mut map = HashMap::new();

    for stage in &job.stages {
        let stage = stage.borrow_mut();
        map.insert(stage.id, StageStatus::Pending);
    }

    // loop until all stages are complete
    let mut num_completed = 0;
    while num_completed < job.stages.len() {
        num_completed = 0;

        //TODO do stages in parallel when possible
        for stage in &job.stages {
            let stage = stage.borrow_mut();
            let status = map.get(&stage.id).unwrap();
            match status {
                StageStatus::Completed => {
                    num_completed += 1;
                }
                StageStatus::Pending => {
                    // have prior stages already completed ?
                    if stage.prior_stages.iter().all(|id| match map.get(id) {
                        Some(StageStatus::Completed) => true,
                        _ => false,
                    }) {
                        println!("Running stage {}", stage.id);
                        let plan = stage
                            .plan
                            .as_ref()
                            .expect("all stages should have plans at execution time");

                        let exec = plan.as_execution_plan();
                        let parts = exec.output_partitioning().partition_count();

                        //TODO do partitions in parallel
                        let mut results = vec![];
                        for partition in 0..parts {
                            println!("Running stage {} partition {}", stage.id, partition);
                            let task = Task::new(job.id, stage.id, 0, 0, plan);

                            //TODO balance load across the executors
                            let exec = &executors[0];

                            results.push(exec.execute_task(&task).await?);
                        }

                        map.insert(stage.id, StageStatus::Completed);

                        if stage.id == job.root_stage_id {
                            let data = executors[0].collect(&results[0])?;
                            return Ok(data);
                        }
                    } else {
                        println!("Cannot run stage {} yet", stage.id);
                    }
                }
            }
        }
    }

    Err(ballista_error("oops"))
}

#[derive(Debug)]
struct DumbShuffleManager {

}

impl ShuffleManager for DumbShuffleManager {
    fn get_executor_id(&self, stage_id: usize, partition_id: usize) -> Result<usize> {
        unimplemented!()
    }
}

pub struct Scheduler {
    job: Job,
    next_stage_id: usize,
    shuffle_manager: Arc<dyn ShuffleManager>
}

impl Scheduler {
    fn new() -> Self {
        let job = Job {
            id: Uuid::new_v4(),
            stages: vec![],
            root_stage_id: 0,
        };
        Self {
            job,
            next_stage_id: 0,
            shuffle_manager: Arc::new(DumbShuffleManager {})
        }
    }

    fn create_job(&mut self, plan: Arc<PhysicalPlan>) -> Result<()> {
        let new_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let new_stage = Rc::new(RefCell::new(Stage::new(new_stage_id)));
        self.job.stages.push(new_stage.clone());
        self.job.root_stage_id = new_stage_id;
        let plan = self.visit_plan(plan, new_stage.clone())?;
        new_stage.as_ref().borrow_mut().plan = Some(plan);
        Ok(())
    }

    fn visit_plan(
        &mut self,
        plan: Arc<PhysicalPlan>,
        current_stage: Rc<RefCell<Stage>>,
    ) -> Result<Arc<PhysicalPlan>> {
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
                Ok(Arc::new(PhysicalPlan::ShuffleReader(Arc::new(
                    ShuffleReaderExec {
                        stage_id: new_stage_id,
                        shuffle_manager: self.shuffle_manager.clone()
                    },
                ))))
            }
            PhysicalPlan::HashAggregate(exec) => {
                let child = self.visit_plan(exec.child.clone(), current_stage)?;
                Ok(Arc::new(PhysicalPlan::HashAggregate(Arc::new(
                    exec.with_new_children(vec![child]),
                ))))
            }
            PhysicalPlan::ParquetScan(_) => Ok(plan.clone()),
            PhysicalPlan::InMemoryTableScan(_) => Ok(plan.clone()),
            other => Err(ballista_error(&format!("visit_plan {:?}", other))),
        }
    }
}

/// Convert a logical plan into a physical plan
pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Arc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, expr, .. } => {
            let exec = ProjectionExec::try_new(expr, create_physical_plan(input)?)?;
            Ok(Arc::new(PhysicalPlan::Projection(Arc::new(exec))))
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
                Ok(Arc::new(PhysicalPlan::HashAggregate(Arc::new(exec))))
            } else {
                // Create partial hash aggregate to run against partitions in parallel
                let partial_hash_exec = HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    group_expr.clone(),
                    aggr_expr.clone(),
                    input,
                )?;
                let partial = Arc::new(PhysicalPlan::HashAggregate(Arc::new(partial_hash_exec)));
                // Create final hash aggregate to run on the coalesced partition of the results
                // from the partial hash aggregate
                // TODO these are not the correct expressions being passed in here for the final agg
                let final_hash_exec = HashAggregateExec::try_new(
                    AggregateMode::Final,
                    group_expr.clone(),
                    aggr_expr.clone(),
                    partial,
                )?;
                Ok(Arc::new(PhysicalPlan::HashAggregate(Arc::new(
                    final_hash_exec,
                ))))
            }
        }
        LogicalPlan::ParquetScan { path, .. } => {
            let exec = ParquetScanExec::try_new(&path, None)?;
            Ok(Arc::new(PhysicalPlan::ParquetScan(Arc::new(exec))))
        }
        other => Err(BallistaError::General(format!("unsupported {:?}", other))),
    }
}

/// Optimizer rule to insert shuffles as needed
pub fn ensure_requirements(plan: &PhysicalPlan) -> Result<Arc<PhysicalPlan>> {
    let execution_plan = plan.as_execution_plan();
    let required_child_dist = execution_plan.required_child_distribution();

    println!(
        "ensure_requirements: {:?} .. required_child_dist={:?}",
        plan, required_child_dist
    );

    // recurse down and replace children
    if execution_plan.children().is_empty() {
        println!("no children");
        return Ok(Arc::new(plan.clone()));
    }

    let children: Vec<Arc<PhysicalPlan>> = execution_plan
        .children()
        .iter()
        .map(|c| ensure_requirements(c.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    match required_child_dist {
        Distribution::SinglePartition => {
            let new_children: Vec<Arc<PhysicalPlan>> = children
                .iter()
                .map(|c| {
                    let partition_count = c
                        .as_execution_plan()
                        .output_partitioning()
                        .partition_count();

                    println!("{:?} has {} partitions", c, partition_count);

                    if partition_count > 1 {
                        println!("inserting shuffle");
                        Arc::new(PhysicalPlan::ShuffleExchange(Arc::new(
                            ShuffleExchangeExec::new(
                                c.clone(),
                                Partitioning::UnknownPartitioning(1),
                            ),
                        )))
                    } else {
                        println!("no shuffle needed");
                        c.clone()
                    }
                })
                .collect();

            let x = plan.with_new_children(new_children);
            println!(
                "ensure_requirements: {:?} returning plan with new children {:?}",
                plan, x
            );
            Ok(Arc::new(x))
        }
        _ => {
            println!(
                "ensure_requirements: {:?} returning cloned plan no mods",
                plan
            );

            Ok(Arc::new(plan.clone()))
        }
    }
}
