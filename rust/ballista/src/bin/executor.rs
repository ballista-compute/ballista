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
use ballista::scheduler::etcd::EtcdClient;
use ballista::scheduler::k8s::KubernetesClient;
use ballista::scheduler::standalone::StandaloneClient;
use ballista::scheduler::SchedulerClient;
use ballista::BALLISTA_VERSION;
use ballista::{
    executor::{BallistaExecutor, ExecutorConfig},
    serde::scheduler::ExecutorMeta,
};
use clap::arg_enum;
use log::info;
use structopt::StructOpt;
use tonic::transport::Server;
use uuid::Uuid;

arg_enum! {
    #[derive(Debug)]
    enum Mode {
        K8s,
        Etcd,
        Standalone
    }
}

/// Ballista Rust Executor
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// discovery mode
    #[structopt(short, long, possible_values = &Mode::variants(), case_insensitive = true, default_value = "Standalone")]
    mode: Mode,

    /// Namespace for the ballista cluster that this executor will join.
    #[structopt(long)]
    namespace: Option<String>,

    /// etcd urls for use when discovery mode is `etcd`
    #[structopt(long)]
    etcd_urls: Option<String>,

    /// Registrar host to register with when discovery mode is `standalone`. This is optional
    /// because it is currently possible to run in standalone mode with a single executor and
    /// no registrar.
    #[structopt(long)]
    registrar_host: Option<String>,

    /// Registrar port to register with when discovery mode is `standalone`. This is optional
    /// because it is currently possible to run in standalone mode with a single executor and
    /// no registrar.
    #[structopt(long)]
    registrar_port: Option<usize>,

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
    let namespace = opt.namespace.unwrap_or_else(|| "ballista".to_owned());
    let external_host = opt.external_host.unwrap_or_else(|| "localhost".to_owned());
    let bind_host = opt.bind_host.as_deref().unwrap_or("0.0.0.0");
    let port = opt.port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    let config = ExecutorConfig::new(&external_host, port, opt.concurrent_tasks);
    info!("Running with config: {:?}", config);

    let executor_meta = ExecutorMeta {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        host: external_host,
        port,
    };

    let scheduler: Arc<dyn SchedulerClient> = match opt.mode {
        Mode::Etcd => {
            let etcd_urls = opt.etcd_urls.unwrap_or_else(|| "localhost:2379".to_owned());
            Arc::new(EtcdClient::new(etcd_urls, namespace, executor_meta))
        }
        Mode::Standalone => {
            let registrar_host = opt.registrar_host.unwrap_or_else(|| "localhost".to_owned());
            let registrar_port = opt.registrar_port.unwrap_or(50051);
            Arc::new(
                StandaloneClient::try_new(registrar_host, registrar_port, executor_meta).await?,
            )
        }
        Mode::K8s => Arc::new(KubernetesClient::new(namespace.to_owned(), namespace)),
    };

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
