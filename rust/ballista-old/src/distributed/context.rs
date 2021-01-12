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

//! Distributed execution context.

use std::collections::HashMap;

use crate::distributed::client::execute_action;
use crate::distributed::etcd::etcd_get_executors;
use crate::distributed::executor::{DiscoveryMode, ExecutorConfig};
use crate::distributed::k8s::k8s_get_executors;
use crate::distributed::scheduler::ExecutionTask;
use crate::error::{ballista_error, Result};

use crate::execution::physical_plan::{
    Action, ColumnarBatch, ExecutionContext, ExecutorMeta, ShuffleId,
};
use async_trait::async_trait;

pub struct BallistaContext {
    /// map from shuffle id to executor uuid
    pub(crate) shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    pub(crate) config: ExecutorConfig,
}

impl BallistaContext {
    pub fn new(
        config: &ExecutorConfig,
        shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    ) -> Self {
        Self {
            config: config.clone(),
            shuffle_locations,
        }
    }
}

#[async_trait]
impl ExecutionContext for BallistaContext {
    async fn get_executor_ids(&self) -> Result<Vec<ExecutorMeta>> {
        match &self.config.discovery_mode {
            DiscoveryMode::Etcd => etcd_get_executors(&self.config.etcd_urls, "default").await,
            DiscoveryMode::Kubernetes => k8s_get_executors("default", "ballista").await,
            DiscoveryMode::Standalone => Err(ballista_error("Standalone mode not implemented yet")),
        }
    }

    async fn execute_task(
        &self,
        executor_meta: ExecutorMeta,
        task: ExecutionTask,
    ) -> Result<ShuffleId> {
        // TODO what is the point of returning this info since it is based on input arg?
        let shuffle_id = ShuffleId::new(task.job_uuid, task.stage_id, task.partition_id);

        let _ = execute_action(
            &executor_meta.host,
            executor_meta.port,
            &Action::Execute(task),
        )
        .await?;

        Ok(shuffle_id)
    }

    async fn read_shuffle(&self, shuffle_id: &ShuffleId) -> Result<Vec<ColumnarBatch>> {
        match self.shuffle_locations.get(shuffle_id) {
            Some(executor_meta) => {
                let batches = execute_action(
                    &executor_meta.host,
                    executor_meta.port,
                    &Action::FetchShuffle(*shuffle_id),
                )
                .await?;
                Ok(batches
                    .iter()
                    .map(|b| ColumnarBatch::from_arrow(b))
                    .collect())
            }
            _ => Err(ballista_error(&format!(
                "Failed to resolve executor UUID for shuffle ID {:?}",
                shuffle_id
            ))),
        }
    }

    fn config(&self) -> ExecutorConfig {
        self.config.clone()
    }
}
