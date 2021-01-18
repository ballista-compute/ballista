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
use std::sync::{Arc, Mutex};

use crate::client::BallistaClient;
use crate::error::{BallistaError, Result};
use crate::serde::scheduler::Action;

use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;

#[allow(dead_code)]
pub struct BallistaContextState {
    settings: HashMap<String, String>, // map from shuffle id to executor uuid
                                       // shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
                                       // config: ExecutorConfig
}

impl BallistaContextState {
    pub fn new(settings: HashMap<String, String>) -> Self {
        Self { settings }
    }
}

#[allow(dead_code)]
pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
}

impl Default for BallistaContext {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl BallistaContext {
    pub fn new(settings: HashMap<String, String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(BallistaContextState::new(settings))),
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

/// The Ballista DataFrame is a wrapper around the DataFusion DataFrame and overrides the
/// `collect` method so that the query is executed against Ballista and not DataFusion.
pub struct BallistaDataFrame {
    /// Ballista context state
    state: Arc<Mutex<BallistaContextState>>,
    /// DataFusion DataFrame representing logical query plan
    df: Arc<dyn DataFrame>,
}

impl BallistaDataFrame {
    pub fn from(state: Arc<Mutex<BallistaContextState>>, df: Arc<dyn DataFrame>) -> Self {
        Self { state, df }
    }

    pub async fn collect(&self) -> Result<SendableRecordBatchStream> {
        let mut client = BallistaClient::try_new("localhost", 8000).await?;
        client
            .execute_action(&Action::InteractiveQuery {
                plan: self.df.to_logical_plan(),
                settings: Default::default(),
            })
            .await
    }

    pub async fn select_columns(&self, columns: Vec<&str>) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .select_columns(columns)
                .map_err(BallistaError::from)?,
        ))
    }

    pub async fn select(&self, expr: Vec<Expr>) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.select(expr).map_err(BallistaError::from)?,
        ))
    }

    // pub async fn filter(&self, expr: Expr) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.filter(expr).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.aggregate(group_expr, aggr_expr).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn limit(&self, n: usize) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.limit(n).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn sort(&self, expr: Vec<Expr>) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.sort(expr).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn join(&self, right: Arc<dyn DataFrame>, join_type: JoinType, left_cols: &[&str], right_cols: &[&str]) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.join(right, join_type, left_cols, right_cols).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn repartition(&self, partitioning_scheme: Partitioning) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.repartition(partitioning_scheme).map_err(|e| BallistaError::from(e))
    // }
    //
    // pub async fn schema(&self) -> &DFSchema {
    //     self.df.schema()
    // }
    //
    // pub async fn to_logical_plan(&self) -> LogicalPlan {
    //     self.df.to_logical_plan()
    // }
    //
    // pub async fn explain(&self, verbose: bool) -> Result<Arc<BallistaDataFrame>> {
    //     self.df.explain(verbose).map_err(|e| BallistaError::from(e))
    // }
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
