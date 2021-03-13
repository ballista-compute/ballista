use crate::SchedulerServer;
use warp::{Filter, Rejection};
use anyhow::{Result};
use std::convert::Infallible;
use tonic::{Response, Request};
use ballista_core::{serde::protobuf::{GetExecutorMetadataParams, ExecutorMetadata, GetExecutorMetadataResult, scheduler_grpc_server::{SchedulerGrpc}}};
use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use ballista_core::serde::scheduler::ExecutorMeta;
use std::rc::Rc;
use std::sync::Arc;

pub struct ApiServer<'a> {
    scheduler_server: &'a SchedulerServer
}


impl<'a> ApiServer<'a> {
    pub fn new(scheduler: &'a SchedulerServer) -> Self {
        ApiServer {
            scheduler_server: scheduler
        }
    }

    pub async fn start(&'a self, port: u16) -> Result<()> {
        let scheduler = Arc::new(self.scheduler_server);
        let executors_api = warp::path!("executors")
            .and(warp::get())
            .and_then(|| async {
                Self::list_executors_data(scheduler).await
            });
        let routes = warp::path("api")
            .and(executors_api);

        let host = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        Ok(warp::serve(routes)
            .run(host)
            .await)
    }

    async fn list_executors_data(scheduler_server: Arc<&SchedulerServer>) -> Result<impl warp::Reply, Infallible> {
        let data: Result<Response<GetExecutorMetadataResult>, tonic::Status> = *scheduler_server.get_executors_metadata(Request::new(GetExecutorMetadataParams {})).await;
        let result = data.unwrap();
        let res: &GetExecutorMetadataResult = result.get_ref();
        let vec: &Vec<ExecutorMetadata> = &res.metadata;
        let metadata: Vec<ExecutorMeta> = vec.iter().map(|v: &ExecutorMetadata| {
            return ExecutorMeta {
                host: v.host.clone(),
                port: v.port as u16,
                id: v.id.clone(),
            };
        }).collect();
        Ok(warp::reply::json(&metadata))
    }
}