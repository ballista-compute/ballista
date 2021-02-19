//! Ballista Rust scheduler binary.

use std::{net::SocketAddr, sync::Arc};

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

async fn start_server(
    config_backend: Arc<dyn ConfigBackendClient>,
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

    let client: Arc<dyn ConfigBackendClient> = match opt.config_backend {
        ConfigBackend::Etcd => {
            let etcd = etcd_client::Client::connect(&[opt.etcd_urls], None)
                .await
                .context("Could not connect to etcd")?;
                Arc::new(EtcdClient::new(etcd))
        }
        ConfigBackend::Standalone => {
            // TODO: Use a real file and make path is configurable
            Arc::new(StandaloneClient::try_new_temporary()
                .context("Could not create standalone config backend")?)
        }
    };
    start_server(client, namespace, addr).await?;
    Ok(())
}
