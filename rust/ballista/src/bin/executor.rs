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

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista::flight_service::BallistaFlightService;
use ballista::BALLISTA_VERSION;

use clap::arg_enum;
use structopt::StructOpt;
use tonic::transport::Server;


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

    /// etcd urls for use when discovery mode is `etcd`
    #[structopt(long)]
    etcd_urls: Option<String>,

    #[structopt(long)]
    bind_host: Option<String>,

    #[structopt(long)]
    external_host: Option<String>,

    /// bind port
    #[structopt(short, long, default_value = "8000")]
    port: usize,

    /// max concurrent tasks
    #[structopt(short, long, default_value = "4")]
    concurrent_tasks: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    // let mode = match opt.mode {
    //    Mode::K8s => DiscoveryMode::Kubernetes,
    //    Mode::Etcd => DiscoveryMode::Etcd,
    //    Mode::Standalone => DiscoveryMode::Standalone,
    // };

    let _external_host = opt.external_host.as_deref().unwrap_or("localhost");
    let bind_host = opt.bind_host.as_deref().unwrap_or("0.0.0.0");
    let _etcd_urls = opt.etcd_urls.as_deref().unwrap_or("localhost:2379");
    let port = opt.port;

    // let config = ExecutorConfig::new(mode, &external_host, port, &etcd_urls, opt.concurrent_tasks);
    // println!("Running with config: {:?}", config);

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    // TODO split scheduler and executor into separate processes soon
    // let scheduler: Arc<dyn Scheduler> = Arc::new(BallistaScheduler::new(config.clone()));
    // let executor: Arc<dyn Executor> = Arc::new(BallistaExecutor::new(config));

    let service = BallistaFlightService {}; //::new(/*scheduler, executor*/);
    let server = FlightServiceServer::new(service);
    println!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
