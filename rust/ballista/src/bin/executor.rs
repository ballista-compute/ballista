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

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista::serde::protobuf::{
    self, scheduler_grpc_client::SchedulerGrpcClient, RegisterExecutorParams,
};
use ballista::{
    executor::flight_service::BallistaFlightService,
    executor::{BallistaExecutor, ExecutorConfig},
    scheduler::{standalone::StandaloneClient, SchedulerServer},
    serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    serde::scheduler::ExecutorMeta,
    BALLISTA_VERSION,
};
use futures::future::MaybeDone;
use log::{info, warn};
use structopt::StructOpt;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

/// Ballista Rust Executor
#[derive(StructOpt, Debug)]
#[structopt(name = "executor")]
struct Opt {
    /// Namespace for the ballista cluster that this executor will join.
    #[structopt(long, default_value = "ballista")]
    namespace: String,

    /// Scheduler host.
    #[structopt(long, default_value = "localhost")]
    scheduler_host: String,

    /// Scheduler port.
    #[structopt(long, default_value = "50050")]
    scheduler_port: u16,

    /// Running in local mode will launch a standalone scheduler inside the executor process.
    /// This will create a single-executor cluster, and is useful for development scenarios.
    #[structopt(long)]
    local: bool,

    /// Local IP address to bind to.
    #[structopt(long, default_value = "0.0.0.0")]
    bind_host: String,

    /// Host name or IP address to register with scheduler so that other executors
    /// can connect to this executor.
    #[structopt(long, default_value = "localhost")]
    external_host: String,

    /// Bind port.
    #[structopt(short, long, default_value = "50051")]
    port: u16,

    /// Directory for temporary IPC files
    #[structopt(long)]
    work_dir: Option<String>,

    /// Max concurrent tasks.
    #[structopt(short, long, default_value = "4")]
    concurrent_tasks: usize,
}

async fn registration_loop(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor_meta: ExecutorMeta,
) {
    let executor_meta: protobuf::ExecutorMetadata = executor_meta.into();
    loop {
        info!("Starting registration with scheduler");
        let registration_result = scheduler
            .register_executor(RegisterExecutorParams {
                metadata: Some(executor_meta.clone()),
            })
            .await;
        if let Err(error) = registration_result {
            warn!("Executor registration failed. If this continues to happen the executor might be marked as dead by the scheduler. Error: {}", error);
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse command-line arguments
    let opt = Opt::from_args();
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
        host: external_host,
        port,
    };

    if opt.local {
        info!("Running in local mode. Scheduler will be run in-proc");
        let client = StandaloneClient::try_new_temporary()
            .context("Could not create standalone config backend")?;
        let server = SchedulerGrpcServer::new(SchedulerServer::new(client, namespace));
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
    let executor = Arc::new(BallistaExecutor::new(config, scheduler.clone()));
    let service = BallistaFlightService::new(executor);

    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    let server_future = tokio::spawn(Server::builder().add_service(server).serve(addr));
    tokio::spawn(registration_loop(scheduler, executor_meta));

    server_future
        .await
        .context("Tokio error")?
        .context("Could not start executor server")?;
    Ok(())
}
