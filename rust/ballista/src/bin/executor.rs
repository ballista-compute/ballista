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

use std::sync::Arc;

use ballista::distributed::executor::{BallistaExecutor, DiscoveryMode, Executor, ExecutorConfig};
use ballista::distributed::flight_service::BallistaFlightService;
use ballista::flight::flight_service_server::FlightServiceServer;
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

    /// bind port
    #[structopt(short, long)]
    port: usize,
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

    //TODO make configurable
    let external_host = "localhost";
    let bind_host = "0.0.0.0";
    let port = opt.port;
    let etcd_urls = "localhost:2379";

    let config = ExecutorConfig::new(mode, external_host, port, etcd_urls);

    println!("Running with config: {:?}", config);

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;
    let executor: Arc<dyn Executor> = Arc::new(BallistaExecutor::new(config));
    let service = BallistaFlightService::new(executor);
    let server = FlightServiceServer::new(service);
    println!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
