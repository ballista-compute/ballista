use crate::SchedulerServer;
use tonic::{Request, Response};
use ballista_core::serde::scheduler::ExecutorMeta;
use ballista_core::{serde::protobuf::{GetExecutorMetadataParams, ExecutorMetadata, GetExecutorMetadataResult, scheduler_grpc_server::{SchedulerGrpc}}};
use warp::Rejection;

pub(crate) async fn list_executors_data(data_server: SchedulerServer) -> Result<impl warp::Reply, Rejection> {
    let data: Result<Response<GetExecutorMetadataResult>, tonic::Status> = data_server.get_executors_metadata(Request::new(GetExecutorMetadataParams {})).await;
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
