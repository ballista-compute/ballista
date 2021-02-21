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

//! Ballista Rust scheduler binary.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use ballista::BALLISTA_VERSION;
use ballista::{
    print_version,
    scheduler::{
        state::{ConfigBackendClient, EtcdClient, StandaloneClient},
        ConfigBackend, SchedulerServer,
    },
    serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
};

use log::info;
use tonic::transport::Server;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(
        env!("OUT_DIR"),
        "/scheduler_configure_me_config.rs"
    ));
}
use config::prelude::*;

async fn start_server<T: ConfigBackendClient + Send + Sync + 'static>(
    config_backend: T,
    namespace: String,
    addr: SocketAddr,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    let server = SchedulerGrpcServer::new(SchedulerServer::new(config_backend, namespace));
    Ok(Server::builder()
        .add_service(server)
        .serve(addr)
        .await
        .context("Could not start grpc server")?)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"]).unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }
    println!("{}", opt.namespace);

    let namespace = opt.namespace;
    let bind_host = opt.bind_host;
    let port = opt.port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    match opt.config_backend {
        ConfigBackend::Etcd => {
            let etcd = etcd_client::Client::connect(&[opt.etcd_urls], None)
                .await
                .context("Could not connect to etcd")?;
            let client = EtcdClient::new(etcd);
            start_server(client, namespace, addr).await?;
        }
        ConfigBackend::Standalone => {
            // TODO: Use a real file and make path is configurable
            let client = StandaloneClient::try_new_temporary()
                .context("Could not create standalone config backend")?;
            start_server(client, namespace, addr).await?;
        }
    };
    Ok(())
}
