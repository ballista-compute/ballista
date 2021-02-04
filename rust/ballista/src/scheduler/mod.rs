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

use log::{error, info, warn};
use tonic::{Request, Response};

use crate::error::Result;
use crate::serde::protobuf::{
    scheduler_grpc_server::SchedulerGrpc, ExecutorMetadata, GetExecutorMetadataParams,
    GetExecutorMetadataResult, RegisterExecutorParams, RegisterExecutorResult,
};
use crate::serde::scheduler::ExecutorMeta;

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
}
