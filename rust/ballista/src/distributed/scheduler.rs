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
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::arrow::datatypes::Schema;
use crate::dataframe::{
    avg, count, max, min, sum, CSV_READER_BATCH_SIZE, PARQUET_READER_BATCH_SIZE,
    PARQUET_READER_QUEUE_SIZE,
};
use crate::datafusion::execution::context::ExecutionContext as DFContext;
use crate::datafusion::execution::physical_plan::csv::CsvReadOptions;
use crate::datafusion::logicalplan::LogicalPlan;
use crate::datafusion::logicalplan::{col_index, Expr, LogicalPlanBuilder};
use crate::datafusion::optimizer::optimizer::OptimizerRule;
use crate::distributed::context::BallistaContext;
use crate::distributed::executor::{ExecutorConfig, ShufflePartition};
use crate::error::{ballista_error, BallistaError, Result};
use crate::execution::operators::ProjectionExec;
use crate::execution::operators::ShuffleExchangeExec;
use crate::execution::operators::ShuffleReaderExec;
use crate::execution::operators::{CsvScanExec, HashAggregateExec};
use crate::execution::operators::{FilterExec, ParquetScanExec};
use crate::execution::physical_plan::{
    Action, AggregateMode, ColumnarBatch, Distribution, ExecutionContext, ExecutionPlan,
    ExecutorMeta, Partitioning, PhysicalPlan, ShuffleId,
};

use crate::distributed::client::BallistaClient;
use async_trait::async_trait;
use smol::Task;
use uuid::Uuid;

#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Execute a query and return results
    async fn execute_query(
        &self,
        plan: &LogicalPlan,
        settings: &HashMap<String, String>,
    ) -> Result<ShufflePartition>;
}

pub struct BallistaScheduler {
    config: ExecutorConfig,
}

impl BallistaScheduler {
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Scheduler for BallistaScheduler {
    async fn execute_query(
        &self,
        logical_plan: &LogicalPlan,
        settings: &HashMap<String, String>,
    ) -> Result<ShufflePartition> {
        let settings = settings.to_owned();

        println!("Logical plan:\n{:?}", logical_plan);
        let ctx = DFContext::new();

        // workaround for https://issues.apache.org/jira/browse/ARROW-9542
        let mut rule = ResolveColumnsRule::new();
        let logical_plan = rule.optimize(logical_plan)?;

        let logical_plan = ctx.optimize(&logical_plan)?;
        println!("Optimized logical plan:\n{:?}", logical_plan);

        let config = self.config.clone();
        let handle = thread::spawn(move || {
            smol::run(async {
                let plan: Arc<PhysicalPlan> = create_physical_plan(&logical_plan, &settings)?;
                println!("Physical plan:\n{:?}", plan);

                let plan = ensure_requirements(plan.as_ref())?;
                println!("Optimized physical plan:\n{:?}", plan);

                println!("Settings: {:?}", settings);

                let job = create_job(plan)?;
                job.explain();

                // create new execution contrext specifically for this query
                let ctx = Arc::new(BallistaContext::new(&config, HashMap::new()));

                let batches = execute_job(&job, ctx.clone()).await?;

                Ok(ShufflePartition {
                    schema: batches[0].schema().as_ref().clone(),
                    data: batches
                        .iter()
                        .map(|b| b.to_arrow())
                        .collect::<Result<Vec<_>>>()?,
                })
            })
        });
        match handle.join() {
            Ok(handle) => handle,
            Err(e) => Err(ballista_error(&format!("Executor thread failed: {:?}", e))),
        }
    }
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
pub struct ExecutionTask {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
    pub(crate) plan: PhysicalPlan,
    pub(crate) shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
}

impl ExecutionTask {
    pub fn new(
        job_uuid: Uuid,
        stage_id: usize,
        partition_id: usize,
        plan: PhysicalPlan,
        shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    ) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
            plan,
            shuffle_locations,
        }
    }

    pub fn key(&self) -> String {
        format!("{}.{}.{}", self.job_uuid, self.stage_id, self.partition_id)
    }
}

/// Create a Job (DAG of stages) from a physical execution plan.
pub fn create_job(plan: Arc<PhysicalPlan>) -> Result<Job> {
    let mut scheduler = JobScheduler::new();
    scheduler.create_job(plan)?;
    Ok(scheduler.job)
}

pub struct JobScheduler {
    job: Job,
    next_stage_id: usize,
}

impl JobScheduler {
    fn new() -> Self {
        let job = Job {
            id: Uuid::new_v4(),
            stages: vec![],
            root_stage_id: 0,
        };
        Self {
            job,
            next_stage_id: 0,
        }
    }

    fn create_job(&mut self, plan: Arc<PhysicalPlan>) -> Result<()> {
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
                let n = exec
                    .child
                    .as_execution_plan()
                    .output_partitioning()
                    .partition_count();

                let shuffle_id = (0..n)
                    .map(|n| ShuffleId {
                        job_uuid: self.job.id,
                        stage_id: new_stage_id,
                        partition_id: n,
                    })
                    .collect();
                Ok(Arc::new(PhysicalPlan::ShuffleReader(Arc::new(
                    ShuffleReaderExec::new(exec.schema(), shuffle_id),
                ))))
            }
            PhysicalPlan::HashAggregate(exec) => {
                let child = self.visit_plan(exec.child.clone(), current_stage)?;
                Ok(Arc::new(PhysicalPlan::HashAggregate(Arc::new(
                    exec.with_new_children(vec![child]),
                ))))
            }
            PhysicalPlan::Filter(exec) => {
                let child = self.visit_plan(exec.child.clone(), current_stage)?;
                Ok(Arc::new(PhysicalPlan::Filter(Arc::new(
                    exec.with_new_children(vec![child]),
                ))))
            }
            PhysicalPlan::CsvScan(_) => Ok(plan.clone()),
            PhysicalPlan::ParquetScan(_) => Ok(plan.clone()),
            _ => Err(ballista_error("visit_plan unsupported operator")),
        }
    }
}

enum StageStatus {
    Pending,
    Completed,
}

enum TaskStatus {
    Pending(Instant),
    Running(Instant),
    Completed(ShuffleId),
    Failed(String),
}

#[derive(Debug, Clone)]
struct ExecutorShuffleIds {
    executor_id: String,
    shuffle_ids: Vec<ShuffleId>,
}

/// Execute a job directly against executors as starting point
pub async fn execute_job(job: &Job, ctx: Arc<dyn ExecutionContext>) -> Result<Vec<ColumnarBatch>> {
    let executors = ctx.get_executor_ids().await?;

    println!("Executors: {:?}", executors);

    if executors.is_empty() {
        println!("no executors found");
        return Err(ballista_error("no executors available"));
    }

    let mut shuffle_location_map: HashMap<ShuffleId, ExecutorMeta> = HashMap::new();

    let mut stage_status_map = HashMap::new();

    for stage in &job.stages {
        let stage = stage.borrow_mut();
        stage_status_map.insert(stage.id, StageStatus::Pending);
    }

    // loop until all stages are complete
    let mut num_completed = 0;
    while num_completed < job.stages.len() {
        num_completed = 0;

        //TODO do stages in parallel when possible
        for stage in &job.stages {
            let stage = stage.borrow_mut();
            let status = stage_status_map.get(&stage.id).unwrap();
            match status {
                StageStatus::Pending => {
                    // have prior stages already completed ?
                    if stage
                        .prior_stages
                        .iter()
                        .all(|id| match stage_status_map.get(id) {
                            Some(StageStatus::Completed) => true,
                            _ => false,
                        })
                    {
                        println!("Running stage {}", stage.id);
                        let plan = stage
                            .plan
                            .as_ref()
                            .expect("all stages should have plans at execution time");

                        let stage_start = Instant::now();

                        let exec = plan.as_execution_plan();
                        let parts = exec.output_partitioning().partition_count();

                        // build queue of tasks per executor
                        let mut next_executor_id = 0;
                        let mut executor_tasks = HashMap::new();
                        #[allow(clippy::needless_range_loop)]
                        for i in 0..executors.len() {
                            executor_tasks.insert(executors[i].id.clone(), vec![]);
                        }
                        for partition in 0..parts {
                            let task = ExecutionTask::new(
                                job.id,
                                stage.id,
                                partition,
                                plan.as_ref().clone(),
                                shuffle_location_map.clone(),
                            );

                            // load balance across the executors
                            let executor_meta = &executors[next_executor_id];
                            next_executor_id += 1;
                            if next_executor_id == executors.len() {
                                next_executor_id = 0;
                            }

                            let queue = executor_tasks
                                .get_mut(&executor_meta.id)
                                .expect("executor queue should exist");

                            queue.push(task);
                        }

                        let mut threads = vec![];

                        #[allow(clippy::needless_range_loop)]
                        for i in 0..executors.len() {
                            let executor = executors[i].clone();
                            let queue = executor_tasks
                                .get(&executor.id)
                                .expect("executor queue should exist");
                            let queue = queue.clone();

                            // start thread per executor
                            let handle = thread::spawn(move || {
                                smol::run(async {
                                    Task::blocking(async move {

                                        // create stateful client
                                        let mut client = BallistaClient::try_new(&executor.host, executor.port).await?;

                                        let mut task_status = Vec::with_capacity(queue.len());
                                        for _ in &queue {
                                            task_status.push(TaskStatus::Pending(Instant::now()));
                                        }

                                        let mut shuffle_ids = vec![];
                                        loop {

                                            let mut pending = 0;
                                            let mut running = 0;
                                            let mut completed = 0;
                                            let mut failed = 0;

                                            for status in &task_status {
                                                match status {
                                                    TaskStatus::Pending(_) => pending += 1,
                                                    TaskStatus::Running(_) => running += 1,
                                                    TaskStatus::Completed(_) => completed += 1,
                                                    TaskStatus::Failed(_) => failed += 1,
                                                }
                                            }

                                            println!(
                                                "Executor {} task stats: {} pending, {} running, {} completed, {} failed",
                                                executor.id,
                                                pending,running,completed,failed
                                            );

                                            if failed > 0  {
                                                return Err(ballista_error("At least one task failed and there is no retry capability yet"))
                                            }

                                            if pending ==0 && running==0 {
                                                break;
                                            }

                                            //TODO need to send multiple tasks per network call - this is really inefficient
                                            let mut count = 0;
                                            let start = Instant::now();
                                            for i in 0..task_status.len() {
                                                let min_time_between_checks = 500;
                                                let should_submit = match &task_status[i] {
                                                    TaskStatus::Pending(last_check) => last_check.elapsed().as_millis() > min_time_between_checks,
                                                    TaskStatus::Running(last_check) => last_check.elapsed().as_millis() > min_time_between_checks,
                                                    TaskStatus::Completed(_) => false,
                                                    TaskStatus::Failed(_) => false,
                                                };

                                                if should_submit {
                                                    count += 1;
                                                    let task = queue[i].clone();
                                                    let task_key = task.key();
                                                    match client.execute_action(&Action::Execute(task.clone())).await
                                                    {
                                                        Ok(_) => {
                                                            let shuffle_id = ShuffleId::new(task.job_uuid, task.stage_id, task.partition_id);
                                                            println!("Task {} completed", task_key);
                                                            shuffle_ids.push(shuffle_id);
                                                            task_status[i] = TaskStatus::Completed(shuffle_id)
                                                        }
                                                        Err(e) => {
                                                            let msg = format!("{:?}", e);
                                                            //TODO would be nice to be able to extract status code here
                                                            if msg.contains("AlreadyExists") {
                                                                task_status[i] = TaskStatus::Running(Instant::now())
                                                            } else {
                                                                task_status[i] = TaskStatus::Failed(msg)
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if count > 0 {
                                                println!("Checked status of {} tasks in {} ms", count, start.elapsed().as_millis());
                                            }

                                            // try not to overwhelm network or executors
                                            thread::sleep(Duration::from_millis(100));
                                        }
                                        Ok(ExecutorShuffleIds {
                                            executor_id: executor.id,
                                            shuffle_ids,
                                        })
                                    })
                                    .await
                                })
                            });
                            threads.push(handle);
                        }

                        let mut stage_shuffle_ids: Vec<ExecutorShuffleIds> = vec![];
                        for thread in threads {
                            stage_shuffle_ids.push(thread.join().unwrap()?);
                        }
                        println!(
                            "Stage {} completed in {} ms and produced {} shuffles",
                            stage.id,
                            stage_start.elapsed().as_millis(),
                            stage_shuffle_ids.len()
                        );

                        for executor_shuffle_ids in &stage_shuffle_ids {
                            for shuffle_id in &executor_shuffle_ids.shuffle_ids {
                                let executor = executors
                                    .iter()
                                    .find(|e| e.id == executor_shuffle_ids.executor_id)
                                    .unwrap();
                                shuffle_location_map.insert(*shuffle_id, executor.clone());
                            }
                        }
                        stage_status_map.insert(stage.id, StageStatus::Completed);

                        if stage.id == job.root_stage_id {
                            println!("reading final results from query!");
                            let ctx = Arc::new(BallistaContext::new(
                                &ctx.config(),
                                shuffle_location_map.clone(),
                            ));

                            println!("stage final shuffle ids: {:?}", stage_shuffle_ids);

                            if !stage_shuffle_ids.is_empty() {
                                let final_shuffle_ids = &stage_shuffle_ids[0]; //TODO Can't assume first one
                                let final_shuffle_ids = &final_shuffle_ids.shuffle_ids;
                                if final_shuffle_ids.len() == 1 {
                                    let data = ctx.read_shuffle(&final_shuffle_ids[0]).await?;
                                    return Ok(data);
                                } else {
                                    return Err(ballista_error(&format!(
                                        "final shuffle_ids len = {}",
                                        final_shuffle_ids.len()
                                    )));
                                }
                            } else {
                                return Err(ballista_error(&format!(
                                    "stage shuffle_ids len = {}",
                                    stage_shuffle_ids.len()
                                )));
                            }
                        }
                    } else {
                        println!("Cannot run stage {} yet", stage.id);
                    }
                }
                StageStatus::Completed => {
                    num_completed += 1;
                }
            }
        }
    }

    // unreachable?
    Err(ballista_error("oops"))
}

/// Convert a logical plan into a physical plan
pub fn create_physical_plan(
    plan: &LogicalPlan,
    settings: &HashMap<String, String>,
) -> Result<Arc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, expr, .. } => {
            let exec = ProjectionExec::try_new(expr, create_physical_plan(input, settings)?)?;
            Ok(Arc::new(PhysicalPlan::Projection(Arc::new(exec))))
        }
        LogicalPlan::Selection { input, expr, .. } => {
            let input = create_physical_plan(input, settings)?;
            let exec = FilterExec::new(&input, expr);
            Ok(Arc::new(PhysicalPlan::Filter(Arc::new(exec))))
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } => {
            let input = create_physical_plan(input, settings)?;
            if input
                .as_execution_plan()
                .output_partitioning()
                .partition_count()
                == 1
            {
                let exec = HashAggregateExec::try_new(
                    AggregateMode::Complete,
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

                let mut final_group = vec![];
                for i in 0..group_expr.len() {
                    final_group.push(col_index(i as usize));
                }

                //TODO this is ugly and shows that the design needs revisiting here

                let mut final_aggr = vec![];
                for (i, expr) in aggr_expr.iter().enumerate() {
                    let j = group_expr.len() + i;
                    match expr {
                        Expr::AggregateFunction { name, .. } => {
                            let expr = match name.as_str() {
                                "SUM" => sum(col_index(j)),
                                "MIN" => min(col_index(j)),
                                "MAX" => max(col_index(j)),
                                "AVG" => avg(col_index(j)),
                                "COUNT" => count(col_index(j)),
                                _ => panic!(),
                            };
                            final_aggr.push(expr);
                        }
                        Expr::Alias(expr, alias) => match expr.as_ref() {
                            Expr::AggregateFunction { name, .. } => {
                                let expr = match name.as_str() {
                                    "SUM" => sum(col_index(j)).alias(alias),
                                    "MIN" => min(col_index(j)).alias(alias),
                                    "MAX" => max(col_index(j)).alias(alias),
                                    "AVG" => avg(col_index(j)).alias(alias),
                                    "COUNT" => count(col_index(j)).alias(alias),
                                    _ => panic!(),
                                };
                                final_aggr.push(expr);
                            }
                            _ => panic!(),
                        },
                        _ => panic!(),
                    }
                }

                let final_hash_exec = HashAggregateExec::try_new(
                    AggregateMode::Final,
                    final_group,
                    final_aggr,
                    partial,
                )?;
                Ok(Arc::new(PhysicalPlan::HashAggregate(Arc::new(
                    final_hash_exec,
                ))))
            }
        }
        LogicalPlan::CsvScan {
            path,
            schema,
            has_header,
            projection,
            ..
        } => {
            // TODO this needs more work to re-use the config defaults defined in dataframe.rs
            let batch_size: usize = settings[CSV_READER_BATCH_SIZE].parse().unwrap_or(64 * 1024);
            let options = CsvReadOptions::new().schema(schema).has_header(*has_header);
            let exec = CsvScanExec::try_new(&path, options, projection.clone(), batch_size)?;
            Ok(Arc::new(PhysicalPlan::CsvScan(Arc::new(exec))))
        }
        LogicalPlan::ParquetScan {
            path, projection, ..
        } => {
            // TODO this needs more work to re-use the config defaults defined in dataframe.rs
            let batch_size: usize = settings[PARQUET_READER_BATCH_SIZE]
                .parse()
                .unwrap_or(64 * 1024);
            let queue_size: usize = settings[PARQUET_READER_QUEUE_SIZE].parse().unwrap_or(2);
            let exec = ParquetScanExec::try_new(&path, projection.clone(), batch_size, queue_size)?;
            Ok(Arc::new(PhysicalPlan::ParquetScan(Arc::new(exec))))
        }
        other => Err(BallistaError::General(format!(
            "create_physical_plan unsupported operator {:?}",
            other
        ))),
    }
}

/// Optimizer rule to insert shuffles as needed
pub fn ensure_requirements(plan: &PhysicalPlan) -> Result<Arc<PhysicalPlan>> {
    let execution_plan = plan.as_execution_plan();

    // recurse down and replace children
    if execution_plan.children().is_empty() {
        return Ok(Arc::new(plan.clone()));
    }
    let children: Vec<Arc<PhysicalPlan>> = execution_plan
        .children()
        .iter()
        .map(|c| ensure_requirements(c.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    match execution_plan.required_child_distribution() {
        Distribution::SinglePartition => {
            let new_children: Vec<Arc<PhysicalPlan>> = children
                .iter()
                .map(|c| {
                    if c.as_execution_plan()
                        .output_partitioning()
                        .partition_count()
                        > 1
                    {
                        Arc::new(PhysicalPlan::ShuffleExchange(Arc::new(
                            ShuffleExchangeExec::new(
                                c.clone(),
                                Partitioning::UnknownPartitioning(1),
                            ),
                        )))
                    } else {
                        c.clone()
                    }
                })
                .collect();

            Ok(Arc::new(plan.with_new_children(new_children)))
        }
        _ => Ok(Arc::new(plan.clone())),
    }
}

//TODO the following code is copied from DataFusion and can be removed in the 0.4.0 release

/// Replace UnresolvedColumns with Columns
pub struct ResolveColumnsRule {}

impl ResolveColumnsRule {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ResolveColumnsRule {
    fn default() -> Self {
        ResolveColumnsRule::new()
    }
}

impl OptimizerRule for ResolveColumnsRule {
    fn optimize(&mut self, plan: &LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { input, expr, .. } => {
                Ok(LogicalPlanBuilder::from(&self.optimize(input)?)
                    .project(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?)
            }
            LogicalPlan::Selection { expr, input } => {
                Ok(LogicalPlanBuilder::from(&self.optimize(input)?)
                    .filter(rewrite_expr(expr, &input.schema())?)?
                    .build()?)
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => Ok(LogicalPlanBuilder::from(&self.optimize(input)?)
                .aggregate(
                    rewrite_expr_list(group_expr, &input.schema())?,
                    rewrite_expr_list(aggr_expr, &input.schema())?,
                )?
                .build()?),
            LogicalPlan::Sort { input, expr, .. } => {
                Ok(LogicalPlanBuilder::from(&self.optimize(input)?)
                    .sort(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?)
            }
            _ => Ok(plan.clone()),
        }
    }
}

fn rewrite_expr_list(expr: &[Expr], schema: &Schema) -> datafusion::error::Result<Vec<Expr>> {
    Ok(expr
        .iter()
        .map(|e| rewrite_expr(e, schema))
        .collect::<datafusion::error::Result<Vec<_>>>()?)
}

fn rewrite_expr(expr: &Expr, schema: &Schema) -> datafusion::error::Result<Expr> {
    match expr {
        Expr::Alias(expr, alias) => Ok(rewrite_expr(&expr, schema)?.alias(&alias)),
        Expr::UnresolvedColumn(name) => Ok(Expr::Column(schema.index_of(&name)?)),
        Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
            left: Box::new(rewrite_expr(&left, schema)?),
            op: op.clone(),
            right: Box::new(rewrite_expr(&right, schema)?),
        }),
        Expr::Not(expr) => Ok(Expr::Not(Box::new(rewrite_expr(&expr, schema)?))),
        Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(rewrite_expr(&expr, schema)?))),
        Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(rewrite_expr(&expr, schema)?))),
        Expr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: Box::new(rewrite_expr(&expr, schema)?),
            data_type: data_type.clone(),
        }),
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => Ok(Expr::Sort {
            expr: Box::new(rewrite_expr(&expr, schema)?),
            asc: *asc,
            nulls_first: *nulls_first,
        }),
        Expr::ScalarFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::ScalarFunction {
            name: name.clone(),
            args: rewrite_expr_list(args, schema)?,
            return_type: return_type.clone(),
        }),
        Expr::AggregateFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::AggregateFunction {
            name: name.clone(),
            args: rewrite_expr_list(args, schema)?,
            return_type: return_type.clone(),
        }),
        _ => Ok(expr.clone()),
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
