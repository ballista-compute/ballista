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
use std::sync::Arc;

use crate::error::Result;
use crate::executor::ExecutorConfig;
use crate::serde::scheduler::{ExecutorMeta, ShuffleId};

use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;

pub struct BallistaContext {
    // map from shuffle id to executor uuid
// shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
// config: ExecutorConfig,
}

impl BallistaContext {
    pub fn new(
        _config: &ExecutorConfig,
        _shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    ) -> Self {
        Self {
            // config: config.clone(),
            // shuffle_locations,
        }
    }

    /// Create a context for executing queries against a remote Ballista executor instance
    pub fn remote(_host: &str, _port: usize, _settings: HashMap<&str, &str>) -> Self {
        todo!()
    }

    /// Create a DataFrame representing a Parquet table scan
    pub fn read_parquet(&self, _path: &str) -> Result<Arc<dyn DataFrame>> {
        todo!()
    }

    /// Create a DataFrame representing a CSV table scan
    pub fn read_csv(&self, _path: &str, _options: CsvReadOptions) -> Result<Arc<dyn DataFrame>> {
        todo!()
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(&self, _name: &str, _table: Arc<dyn DataFrame>) -> Result<()> {
        todo!()
    }

    /// Create a DataFrame from a SQL statement
    pub fn sql(&self, _sql: &str) -> Result<Arc<dyn DataFrame>> {
        todo!()
    }

    /// Execute the query and return a stream of result batches
    pub fn execute(&self) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}

// #[async_trait]
// impl ExecutionContext for BallistaContext {
//     async fn get_executor_ids(&self) -> Result<Vec<ExecutorMeta>> {
//         match &self.config.discovery_mode {
//             DiscoveryMode::Etcd => etcd_get_executors(&self.config.etcd_urls, "default").await,
//             DiscoveryMode::Kubernetes => k8s_get_executors("default", "ballista").await,
//             DiscoveryMode::Standalone => Err(ballista_error("Standalone mode not implemented yet")),
//         }
//     }
//
//     async fn execute_task(
//         &self,
//         executor_meta: ExecutorMeta,
//         task: ExecutionTask,
//     ) -> Result<ShuffleId> {
//         // TODO what is the point of returning this info since it is based on input arg?
//         let shuffle_id = ShuffleId::new(task.job_uuid, task.stage_id, task.partition_id);
//
//         let _ = execute_action(
//             &executor_meta.host,
//             executor_meta.port,
//             &Action::Execute(task),
//         )
//         .await?;
//
//         Ok(shuffle_id)
//     }
//
//     async fn read_shuffle(&self, shuffle_id: &ShuffleId) -> Result<Vec<ColumnarBatch>> {
//         match self.shuffle_locations.get(shuffle_id) {
//             Some(executor_meta) => {
//                 let batches = execute_action(
//                     &executor_meta.host,
//                     executor_meta.port,
//                     &Action::FetchShuffle(*shuffle_id),
//                 )
//                 .await?;
//                 Ok(batches
//                     .iter()
//                     .map(|b| ColumnarBatch::from_arrow(b))
//                     .collect())
//             }
//             _ => Err(ballista_error(&format!(
//                 "Failed to resolve executor UUID for shuffle ID {:?}",
//                 shuffle_id
//             ))),
//         }
//     }
//
//     fn config(&self) -> ExecutorConfig {
//         self.config.clone()
//     }
// }
