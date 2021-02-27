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

//! Ballista Rust executor binary.

use std::{convert::TryInto, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista::{
    client::BallistaClient,
    serde::protobuf::{
        self, scheduler_grpc_client::SchedulerGrpcClient, task_status, FailedTask, PartitionId,
        PollWorkParams, TaskStatus,
    },
};
use ballista::{
    executor::flight_service::BallistaFlightService,
    executor::{BallistaExecutor, ExecutorConfig},
    print_version,
    scheduler::{state::StandaloneClient, SchedulerServer},
    serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    serde::scheduler::ExecutorMeta,
    BALLISTA_VERSION,
};
use datafusion::physical_plan::ExecutionPlan;
use futures::future::MaybeDone;
use log::{debug, info, warn};
use protobuf::CompletedTask;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(env!("OUT_DIR"), "/executor_configure_me_config.rs"));
}
use config::prelude::*;

struct CurrentTaskInformation {
    task_id: PartitionId,
    result: Option<ballista::error::Result<()>>,
}

async fn poll_loop(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor_client: BallistaClient,
    executor_meta: ExecutorMeta,
) {
    let executor_meta: protobuf::ExecutorMetadata = executor_meta.into();
    let running_task: Arc<Mutex<Option<CurrentTaskInformation>>> = Arc::new(Mutex::new(None));
    loop {
        debug!("Starting registration with scheduler");
        let mut task_status = vec![];
        let mut running_task_guard = running_task.lock().await;
        if let Some(CurrentTaskInformation { task_id, result }) = &*running_task_guard {
            let task_id = task_id.clone();
            match result {
                Some(Ok(_)) => {
                    info!("Current task finished");
                    *running_task_guard = None;
                    task_status.push(TaskStatus {
                        job_id: task_id.job_id,
                        stage_id: task_id.stage_id,
                        partition_id: task_id.partition_id,
                        status: Some(task_status::Status::Completed(CompletedTask {
                            executor_id: executor_meta.id.clone(),
                        })),
                    });
                }
                Some(Err(e)) => {
                    let error_msg = e.to_string();
                    info!("Current task failed: {}", error_msg);
                    *running_task_guard = None;
                    task_status.push(TaskStatus {
                        job_id: task_id.job_id,
                        stage_id: task_id.stage_id,
                        partition_id: task_id.partition_id,
                        status: Some(task_status::Status::Failed(FailedTask {
                            error: format!("Task failed due to Tokio error: {}", error_msg),
                        })),
                    });
                }
                None => {
                    info!("Current task still in progress");
                }
            }
        }
        let can_accept_task = running_task_guard.is_none();
        drop(running_task_guard);
        let registration_result = scheduler
            .poll_work(PollWorkParams {
                metadata: Some(executor_meta.clone()),
                can_accept_task, // TODO: honor concurrent_tasks setting. right now each execution runs a single task at a time
                task_status,
            })
            .await;
        match registration_result {
            Ok(result) => {
                if let Some(task) = result.into_inner().task {
                    info!("Received task {:?}", task.task_id.as_ref().unwrap());
                    let plan: Arc<dyn ExecutionPlan> = (&task.plan.unwrap()).try_into().unwrap();
                    let task_id = task.task_id.unwrap();
                    // TODO: This is a convoluted way of executing the task. We should move the task
                    // execution code outside of the FlightService (data plane) into the control plane.
                    let mut executor_client = executor_client.clone();
                    let task_id_clone = task_id.clone();
                    let running_task = running_task.clone();
                    tokio::spawn(async move {
                        let r = executor_client
                            .execute_partition(
                                task_id_clone.job_id.clone(),
                                task_id_clone.stage_id as usize,
                                vec![task_id_clone.partition_id as usize],
                                plan,
                            )
                            .await;
                        info!("DONE WITH CURRENT TASK: {:?}", r);
                        let mut running_task = running_task.lock().await;
                        *running_task = Some(CurrentTaskInformation {
                            task_id: task_id_clone,
                            result: Some(r.map(|_| ())),
                        });
                    });
                }
            }
            Err(error) => {
                warn!("Executor registration failed. If this continues to happen the executor might be marked as dead by the scheduler. Error: {}", error);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse command-line arguments
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/executor.toml"]).unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let namespace = opt.namespace;
    let external_host = opt.external_host;
    let bind_host = opt.bind_host;
    let port = opt.port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {}", addr))?;

    let scheduler_host = if opt.local {
        external_host.to_owned()
    } else {
        opt.scheduler_host
    };
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{}:{}", scheduler_host, scheduler_port);

    let work_dir = opt.work_dir.unwrap_or(
        TempDir::new()?
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap(),
    );
    let config = ExecutorConfig::new(&external_host, port, &work_dir, opt.concurrent_tasks);
    info!("Running with config: {:?}", config);

    let executor_meta = ExecutorMeta {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        host: external_host.clone(),
        port,
    };

    if opt.local {
        info!("Running in local mode. Scheduler will be run in-proc");
        let client = StandaloneClient::try_new_temporary()
            .context("Could not create standalone config backend")?;
        let server = SchedulerGrpcServer::new(SchedulerServer::new(Arc::new(client), namespace));
        let addr = format!("{}:{}", bind_host, scheduler_port);
        let addr = addr
            .parse()
            .with_context(|| format!("Could not parse {}", addr))?;
        info!(
            "Ballista v{} Rust Scheduler listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let scheduler_future = tokio::spawn(Server::builder().add_service(server).serve(addr));
        let mut scheduler_result = futures::future::maybe_done(scheduler_future);

        // Ensure scheduler is ready to receive connections
        while SchedulerGrpcClient::connect(scheduler_url.clone())
            .await
            .is_err()
        {
            let scheduler_future = match scheduler_result {
                MaybeDone::Future(f) => f,
                MaybeDone::Done(Err(e)) => return Err(e).context("Tokio error"),
                MaybeDone::Done(Ok(Err(e))) => {
                    return Err(e).context("Scheduler failed to initialize correctly")
                }
                MaybeDone::Done(Ok(Ok(()))) => {
                    return Err(anyhow::format_err!(
                        "Scheduler unexpectedly finished successfully"
                    ))
                }
                MaybeDone::Gone => panic!("Received Gone from recently created MaybeDone"),
            };
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            scheduler_result = futures::future::maybe_done(scheduler_future);
        }
    }

    let scheduler = SchedulerGrpcClient::connect(scheduler_url)
        .await
        .context("Could not connect to scheduler")?;
    let executor = Arc::new(BallistaExecutor::new(config));
    let service = BallistaFlightService::new(executor);

    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    let server_future = tokio::spawn(Server::builder().add_service(server).serve(addr));
    let client = BallistaClient::try_new(&external_host, port).await?;
    tokio::spawn(poll_loop(scheduler, client, executor_meta));

    server_future
        .await
        .context("Tokio error")?
        .context("Could not start executor server")?;
    Ok(())
}
