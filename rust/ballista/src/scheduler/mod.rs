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

//! Support for distributed schedulers, such as Kubernetes

pub mod etcd;
pub mod planner;
pub mod standalone;

use std::convert::TryInto;

use crate::error::Result;
use crate::executor::shuffle_reader::ShuffleReaderExec;
use crate::scheduler::planner::DistributedPlanner;
use crate::serde::protobuf::{
    scheduler_grpc_server::SchedulerGrpc, ExecuteQueryParams, ExecuteQueryResult, ExecutorMetadata,
    GetExecutorMetadataParams, GetExecutorMetadataResult, PartitionLocation,
    RegisterExecutorParams, RegisterExecutorResult,
};
use crate::serde::scheduler::ExecutorMeta;

use datafusion::execution::context::ExecutionContext;
use log::{error, info, warn};
use tonic::{Request, Response};

/// A trait that contains the necessary methods to save and retrieve the state and configuration of a cluster.
#[tonic::async_trait]
pub trait ConfigBackendClient: Clone {
    /// Retrieve the data associated with a specific key.
    ///
    /// An empty vec is returned if the key does not exist.
    async fn get(&mut self, key: &str) -> Result<Vec<u8>>;

    /// Retrieve all data associated with a specific key.
    async fn get_from_prefix(&mut self, prefix: &str) -> Result<Vec<Vec<u8>>>;

    /// Saves the value into the provided key, overriding any previous data that might have been associated to that key.
    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<()>;
}

pub struct SchedulerServer<Config: ConfigBackendClient> {
    client: Config,
    namespace: String,
}

impl<Config: ConfigBackendClient> SchedulerServer<Config> {
    pub fn new(client: Config, namespace: String) -> Self {
        Self { client, namespace }
    }
}

#[tonic::async_trait]
impl<T: ConfigBackendClient + Send + Sync + 'static> SchedulerGrpc for SchedulerServer<T> {
    async fn get_executors_metadata(
        &self,
        _request: Request<GetExecutorMetadataParams>,
    ) -> std::result::Result<Response<GetExecutorMetadataResult>, tonic::Status> {
        info!("Received get_executors_metadata request");
        let mut client = self.client.clone();
        let result = client
            .get_from_prefix(&self.namespace)
            .await
            .map_err(|e| {
                let msg = format!("Could not retrieve data from configuration store: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .into_iter()
            .map(|bytes| serde_json::from_slice::<ExecutorMeta>(&bytes))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                let msg = format!("Could not deserialize etcd value: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .into_iter()
            .map(|meta| meta.into())
            .collect();
        Ok(Response::new(GetExecutorMetadataResult {
            metadata: result,
        }))
    }

    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> std::result::Result<Response<RegisterExecutorResult>, tonic::Status> {
        if let RegisterExecutorParams {
            metadata: Some(metadata),
        } = request.into_inner()
        {
            info!("Received register_executor request for {:?}", metadata);
            let ExecutorMetadata { id, host, port } = metadata;
            let key = format!("/ballista/{}/{}", self.namespace, id);
            let value = format!("{}:{}", host, port);
            self.client
                .clone()
                .put(key, value.into_bytes())
                .await
                .map_err(|e| {
                    let msg = format!("Could not put etcd value: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;
            Ok(Response::new(RegisterExecutorResult {}))
        } else {
            warn!("Received invalid executor registration request");
            Err(tonic::Status::invalid_argument(
                "Missing metadata in request",
            ))
        }
    }

    async fn execute_logical_plan(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> std::result::Result<Response<ExecuteQueryResult>, tonic::Status> {
        if let ExecuteQueryParams {
            logical_plan: Some(x),
        } = request.into_inner()
        {
            let mut client = self.client.clone();
            let executors = client
                .get_from_prefix(&self.namespace)
                .await
                .map_err(|e| {
                    let msg = format!("Could not retrieve data from configuration store: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?
                .into_iter()
                .map(|bytes| serde_json::from_slice::<ExecutorMeta>(&bytes))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    let msg = format!("Could not deserialize etcd value: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;

            // parse protobuf
            let plan = (&x).try_into().map_err(|e| {
                let msg = format!("Could not parse logical plan protobuf: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

            // create physical plan using DataFusion
            let datafusion_ctx = ExecutionContext::new();
            let plan = datafusion_ctx
                .optimize(&plan)
                .and_then(|plan| datafusion_ctx.create_physical_plan(&plan))
                .map_err(|e| {
                    let msg = format!("Could not retrieve data from configuration store: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;

            // create distributed physical plan using Ballista
            let mut planner = DistributedPlanner::new(executors);
            let plan = planner.execute_distributed_query(plan).await.map_err(|e| {
                let msg = format!("Could not execute distributed plan: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

            if let Some(plan) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
                let mut partition_location = vec![];
                for loc in &plan.partition_location {
                    partition_location.push(PartitionLocation {
                        partition_id: Some(loc.partition_id.try_into().map_err(|e| {
                            let msg = format!("Could not execute distributed plan: {}", e);
                            error!("{}", msg);
                            tonic::Status::internal(msg)
                        })?),
                        executor_meta: Some(loc.executor_meta.clone().try_into().map_err(|e| {
                            let msg = format!("Could not execute distributed plan: {}", e);
                            error!("{}", msg);
                            tonic::Status::internal(msg)
                        })?),
                    });
                }
                Ok(Response::new(ExecuteQueryResult { partition_location }))
            } else {
                Err(tonic::Status::internal(
                    "Expected plan final operator to be ShuffleReaderExec",
                ))
            }
        } else {
            Err(tonic::Status::internal("Error parsing request"))
        }
    }
}
