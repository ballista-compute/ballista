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

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista::flight_service::BallistaFlightService;
use ballista::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista::BALLISTA_VERSION;
use ballista::{
    executor::{BallistaExecutor, ExecutorConfig},
    serde::scheduler::ExecutorMeta,
};
use log::info;
use structopt::StructOpt;
use tonic::transport::Server;
use uuid::Uuid;

/// Ballista Rust Executor
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Namespace for the ballista cluster that this executor will join.
    #[structopt(long)]
    namespace: Option<String>,

    /// Registrar host to register with when discovery mode is `standalone`. This is optional
    /// because it is currently possible to run in standalone mode with a single executor and
    /// no registrar.
    #[structopt(long)]
    registrar_host: Option<String>,

    /// Registrar port to register with when discovery mode is `standalone`. This is optional
    /// because it is currently possible to run in standalone mode with a single executor and
    /// no registrar.
    #[structopt(long, default_value = "50050")]
    registrar_port: u16,

    /// Local host name or IP address to bind to
    #[structopt(long)]
    bind_host: Option<String>,

    /// Host name or IP address to register with scheduler so that other executors
    /// can connect to this executor
    #[structopt(long)]
    external_host: Option<String>,

    /// bind port
    #[structopt(short, long, default_value = "50051")]
    port: u16,

    /// max concurrent tasks
    #[structopt(short, long, default_value = "4")]
    concurrent_tasks: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // parse command-line arguments
    let opt = Opt::from_args();
    let _namespace = opt.namespace.unwrap_or_else(|| "ballista".to_owned());
    let external_host = opt.external_host.unwrap_or_else(|| "localhost".to_owned());
    let bind_host = opt.bind_host.as_deref().unwrap_or("0.0.0.0");
    let port = opt.port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    let registrar_host = opt.registrar_host.unwrap_or_else(|| "localhost".to_owned());
    let registrar_port = opt.registrar_port;

    let config = ExecutorConfig::new(&external_host, port, opt.concurrent_tasks);
    info!("Running with config: {:?}", config);

    let _executor_meta = ExecutorMeta {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        host: external_host,
        port,
    };

    let scheduler =
        SchedulerGrpcClient::connect(format!("http://{}:{}", registrar_host, registrar_port))
            .await?;
    // TODO: register metadata
    let executor = Arc::new(BallistaExecutor::new(config, scheduler));
    let service = BallistaFlightService::new(executor);
    let server = FlightServiceServer::new(service);
    info!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
