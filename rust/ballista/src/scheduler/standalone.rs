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

use std::time::Duration;

use crate::error::{BallistaError, Result};
use crate::scheduler::SchedulerClient;
use crate::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use crate::serde::protobuf::{GetExecutorMetadataParams, RegisterExecutorParams};
use crate::serde::scheduler::ExecutorMeta;

use async_trait::async_trait;
use log::warn;
use tonic::transport::Channel;

pub struct StandaloneClient {
    client: SchedulerGrpcClient<Channel>,
}

impl StandaloneClient {
    pub async fn try_new(
        registrar_url: String,
        registrar_port: usize,
        executor_meta: ExecutorMeta,
    ) -> Result<Self> {
        let endpoint = format!("http://{}:{}", registrar_url, registrar_port);
        let client = SchedulerGrpcClient::connect(endpoint).await?;
        tokio::spawn(Self::refresh_loop(client.clone(), executor_meta));

        Ok(Self { client })
    }

    async fn refresh_loop(mut client: SchedulerGrpcClient<Channel>, executor_meta: ExecutorMeta) {
        loop {
            let result = client
                .register_executor(RegisterExecutorParams {
                    metadata: Some(executor_meta.clone().into()),
                })
                .await;
            if let Err(e) = result {
                warn!("Received error when trying to register executor: {}", e);
            }

            tokio::time::delay_for(Duration::from_secs(15)).await;
        }
    }
}

#[async_trait]
impl SchedulerClient for StandaloneClient {
    async fn get_executors(&self) -> Result<Vec<ExecutorMeta>> {
        Ok(self
            .client
            .clone()
            .get_executors_metadata(GetExecutorMetadataParams {})
            .await
            .map_err(|e| {
                BallistaError::General(format!("Grpc error while getting executor metadata: {}", e))
            })?
            .into_inner()
            .metadata
            .into_iter()
            .map(ExecutorMeta::from)
            .collect())
    }
}
