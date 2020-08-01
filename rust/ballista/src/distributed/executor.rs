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

//! Core executor logic for executing queries and storing results in memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::distributed::context::BallistaContext;
use crate::distributed::etcd::start_etcd_thread;
use crate::distributed::scheduler::ExecutionTask;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::ShuffleId;

use async_trait::async_trait;
use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
use smol::Task;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub(crate) discovery_mode: DiscoveryMode,
    pub(crate) host: String,
    pub(crate) port: usize,
    pub(crate) etcd_urls: String,
    pub(crate) concurrent_tasks: usize,
}

impl ExecutorConfig {
    pub fn new(
        discovery_mode: DiscoveryMode,
        host: &str,
        port: usize,
        etcd_urls: &str,
        concurrent_tasks: usize,
    ) -> Self {
        Self {
            discovery_mode,
            host: host.to_owned(),
            port,
            etcd_urls: etcd_urls.to_owned(),
            concurrent_tasks,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    Etcd,
    Kubernetes,
    Standalone,
}

#[derive(Clone)]
pub struct ShufflePartition {
    pub(crate) schema: Schema,
    pub(crate) data: Vec<RecordBatch>,
}

#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute a query and store the resulting shuffle partitions in memory
    fn submit_task(&self, task: &ExecutionTask) -> Result<TaskStatus>;

    /// Collect the results of a prior task that resulted in a shuffle partition
    fn collect(&self, shuffle_id: &ShuffleId) -> Result<ShufflePartition>;
}

#[derive(Debug, Clone)]
pub enum TaskStatus {
    /// The task has been accepted by the executor but is not running yet
    Pending,
    /// The task is running on one of the worker threads
    Running,
    /// The task has completed
    Completed,
    /// The task has failed
    Failed(String),
}

pub struct BallistaExecutor {
    /// Task status
    pub(crate) task_status_map: Arc<Mutex<HashMap<String, TaskStatus>>>,
    /// Results from executing a task
    shuffle_partitions: Arc<Mutex<HashMap<String, ShufflePartition>>>,
    /// Channel for submitting tasks to workers
    tx: Sender<ExecutionTask>,
}

impl BallistaExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        let uuid = Uuid::new_v4();

        match &config.discovery_mode {
            DiscoveryMode::Etcd => {
                println!("Running in etcd mode");
                start_etcd_thread(
                    &config.etcd_urls,
                    "default",
                    &uuid,
                    &config.host,
                    config.port,
                );
            }
            DiscoveryMode::Kubernetes => println!("Running in k8s mode"),
            DiscoveryMode::Standalone => println!("Running in standalone mode"),
        }

        let task_status_map = Arc::new(Mutex::new(HashMap::new()));
        let shuffle_partitions = Arc::new(Mutex::new(HashMap::new()));

        let (tx, rx): (Sender<ExecutionTask>, Receiver<ExecutionTask>) = unbounded();

        // launch worker threads
        for _ in 0..config.concurrent_tasks {
            let rx = rx.clone();
            let config = config.clone();
            let task_status_map = task_status_map.clone();
            let shuffle_partitions = shuffle_partitions.clone();
            thread::spawn(move || {
                smol::run(async {
                    Task::blocking(async move {
                        loop {
                            match rx.recv() {
                                Ok(task) => {
                                    let shuffle_id = ShuffleId::new(
                                        task.job_uuid,
                                        task.stage_id,
                                        task.partition_id,
                                    );

                                    set_task_status(
                                        &task_status_map,
                                        &task.key(),
                                        TaskStatus::Running,
                                    );

                                    match execute_task(&config, &task).await {
                                        Ok((schema, batches)) => {
                                            let key = format!(
                                                "{}:{}:{}",
                                                shuffle_id.job_uuid,
                                                shuffle_id.stage_id,
                                                shuffle_id.partition_id
                                            );
                                            let mut shuffle_partitions = shuffle_partitions
                                                .lock()
                                                .expect("failed to lock mutex");

                                            shuffle_partitions.insert(
                                                key,
                                                ShufflePartition {
                                                    schema,
                                                    data: batches,
                                                },
                                            );
                                            set_task_status(
                                                &task_status_map,
                                                &task.key(),
                                                TaskStatus::Completed,
                                            );
                                        }
                                        Err(e) => {
                                            println!("Task failed: {:?}", e);
                                            set_task_status(
                                                &task_status_map,
                                                &task.key(),
                                                TaskStatus::Failed(e.to_string()),
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("Executor thread terminated due to error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    })
                    .await
                })
            });
        }

        Self {
            task_status_map,
            shuffle_partitions,
            tx,
        }
    }
}

fn set_task_status(
    task_status_map: &Arc<Mutex<HashMap<String, TaskStatus>>>,
    task_key: &str,
    task_status: TaskStatus,
) {
    let mut map = task_status_map.lock().expect("failed to lock mutex");
    map.insert(task_key.to_owned(), task_status);
}

async fn execute_task(
    config: &ExecutorConfig,
    task: &ExecutionTask,
) -> Result<(Schema, Vec<RecordBatch>)> {
    // create new execution context specifically for this query
    let ctx = Arc::new(BallistaContext::new(
        &config,
        task.shuffle_locations.clone(),
    ));

    let exec_plan = task.plan.as_execution_plan();
    let stream = exec_plan.execute(ctx, task.partition_id).await?;
    let mut batches = vec![];
    while let Some(batch) = stream.next().await? {
        batches.push(batch.to_arrow()?);
    }

    Ok((stream.schema().as_ref().to_owned(), batches))
}

#[async_trait]
impl Executor for BallistaExecutor {
    fn submit_task(&self, task: &ExecutionTask) -> Result<TaskStatus> {
        // is it already submitted?
        {
            let task_status = self.task_status_map.lock().expect("failed to lock mutex");
            if let Some(status) = task_status.get(&task.key()) {
                return Ok(status.to_owned());
            }
        }

        match self.tx.send(task.to_owned()) {
            Ok(_) => {
                set_task_status(&self.task_status_map, &task.key(), TaskStatus::Pending);
                Ok(TaskStatus::Pending)
            }
            Err(_) => {
                set_task_status(
                    &self.task_status_map,
                    &task.key(),
                    TaskStatus::Failed("could not submit".to_owned()),
                );
                Err(ballista_error("send error"))
            }
        }
    }

    fn collect(&self, shuffle_id: &ShuffleId) -> Result<ShufflePartition> {
        let key = format!(
            "{}:{}:{}",
            shuffle_id.job_uuid, shuffle_id.stage_id, shuffle_id.partition_id
        );
        let mut shuffle_partitions = self
            .shuffle_partitions
            .lock()
            .expect("failed to lock mutex");
        match shuffle_partitions.remove(&key) {
            Some(partition) => Ok(partition),
            _ => Err(ballista_error(&format!(
                "invalid shuffle partition id {}",
                key
            ))),
        }
    }
}
