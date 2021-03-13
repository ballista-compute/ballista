mod handlers;

use crate::SchedulerServer;
use warp::{Filter, Rejection};
use anyhow::{Result};
use std::convert::Infallible;
use tonic::{Response, Request};
use ballista_core::{serde::protobuf::{GetExecutorMetadataParams, ExecutorMetadata, GetExecutorMetadataResult, scheduler_grpc_server::{SchedulerGrpc}}};
use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use ballista_core::serde::scheduler::ExecutorMeta;
use std::sync::{Arc, RwLock};

pub type DataServer = Arc<RwLock<SchedulerServer>>;

pub async fn start_api_server(ds: &'static DataServer, host: [u8; 4], port: u16) -> Result<()> {
    let executors_api = warp::path!("executors")
        .and(warp::get())
        .and_then(move || async move {
            handlers::list_executors_data(Arc::clone(ds)).await
        });
    let routes = warp::path("api")
        .and(executors_api);

    Ok(warp::serve(routes)
        .run((host, port))
        .await)
}
