//! Ballista Rust scheduler binary.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use ballista::BALLISTA_VERSION;
use ballista::{
    scheduler::{
        state::{etcd::EtcdClient, standalone::StandaloneClient, ConfigBackendClient},
        SchedulerServer,
    },
    serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
};
use clap::arg_enum;
use log::info;
use structopt::StructOpt;
use tonic::transport::Server;

arg_enum! {
    #[derive(Debug)]
    enum ConfigBackend {
        Etcd,
        Standalone
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "scheduler")]
struct Opt {
    /// The configuration backend for the scheduler.
    #[structopt(short, long, possible_values = &ConfigBackend::variants(), case_insensitive = true, default_value = "Standalone")]
    config_backend: ConfigBackend,

    /// Namespace for the ballista cluster that this scheduler will join.
    #[structopt(long, default_value = "ballista")]
    namespace: String,

    /// etcd urls for use when discovery mode is `etcd`
    #[structopt(long, default_value = "localhost:2379")]
    etcd_urls: String,

    /// Local host name or IP address to bind to
    #[structopt(long, default_value = "0.0.0.0")]
    bind_host: String,

    /// bind port
    #[structopt(short, long, default_value = "50050")]
    port: u16,
}

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

    // parse command-line arguments
    let opt = Opt::from_args();
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
