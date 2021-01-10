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

use ballista::arrow_flight::flight_service_server::FlightServiceServer;
use ballista::distributed::executor::{BallistaExecutor, DiscoveryMode, Executor, ExecutorConfig};
use ballista::distributed::flight_service::BallistaFlightService;
use ballista::distributed::scheduler::{BallistaScheduler, Scheduler};
use ballista::BALLISTA_VERSION;

use structopt::StructOpt;
use tonic::transport::Server;

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// discovery mode
    #[structopt(short, long)]
    mode: Option<String>,

    /// etcd urls for use when discovery mode is `etcd`
    #[structopt(long)]
    etcd_urls: Option<String>,

    #[structopt(long)]
    bind_host: Option<String>,

    #[structopt(long)]
    external_host: Option<String>,

    /// bind port
    #[structopt(short, long)]
    port: usize,

    /// max concurrent tasks
    #[structopt(short, long)]
    concurrent_tasks: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let mode = match opt.mode {
        Some(s) => match s.as_str() {
            "k8s" => DiscoveryMode::Kubernetes,
            "etcd" => DiscoveryMode::Etcd,
            _ => unimplemented!(),
        },
        _ => DiscoveryMode::Standalone,
    };

    let external_host = opt.external_host.unwrap_or_else(|| "localhost".to_owned());
    let bind_host = opt.bind_host.unwrap_or_else(|| "0.0.0.0".to_owned());
    let etcd_urls = opt.etcd_urls.unwrap_or_else(|| "localhost:2379".to_owned());
    let port = opt.port;

    let config = ExecutorConfig::new(mode, &external_host, port, &etcd_urls, opt.concurrent_tasks);

    println!("Running with config: {:?}", config);

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    // TODO split scheduler and executor into separate processes soon
    let scheduler: Arc<dyn Scheduler> = Arc::new(BallistaScheduler::new(config.clone()));
    let executor: Arc<dyn Executor> = Arc::new(BallistaExecutor::new(config));

    let service = BallistaFlightService::new(scheduler, executor);
    let server = FlightServiceServer::new(service);
    println!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
