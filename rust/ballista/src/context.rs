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
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{any::Any, pin::Pin};

use crate::client::BallistaClient;
use crate::error::{BallistaError, Result};
use crate::serde::scheduler::{Action, ExecutorMeta};

use crate::scheduler::planner::DistributedPlanner;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{DFSchema, Expr, LogicalPlan, Partitioning};
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{dataframe::DataFrame, physical_plan::RecordBatchStream};
use log::{debug, info};

#[derive(Debug)]

pub enum ClusterMeta {
    Direct { host: String, port: usize }, //TODO add etcd and k8s options here
}

#[allow(dead_code)]

pub struct BallistaContextState {
    /// Meta-data required for connecting to a scheduler instances in the cluster
    cluster_meta: ClusterMeta,
    /// Tables that have been registered with this context
    tables: HashMap<String, LogicalPlan>,
    /// General purpose settings
    settings: HashMap<String, String>,
}

impl BallistaContextState {
    pub fn new(cluster_meta: ClusterMeta, settings: HashMap<String, String>) -> Self {
        Self {
            cluster_meta,
            tables: HashMap::new(),
            settings,
        }
    }
}

#[allow(dead_code)]

pub struct BallistaContext {
    state: Arc<Mutex<BallistaContextState>>,
}

impl BallistaContext {
    /// Create a context for executing queries against a remote Ballista executor instance
    pub fn remote(host: &str, port: usize, settings: HashMap<String, String>) -> Self {
        let meta = ClusterMeta::Direct {
            host: host.to_owned(),
            port,
        };
        let state = BallistaContextState::new(meta, settings);

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Create a DataFrame representing a Parquet table scan

    pub fn read_parquet(&self, path: &str) -> Result<BallistaDataFrame> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        let df = ctx.read_parquet(path.to_str().unwrap())?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }

    /// Create a DataFrame representing a CSV table scan

    pub fn read_csv(&self, path: &str, options: CsvReadOptions) -> Result<BallistaDataFrame> {
        // convert to absolute path because the executor likely has a different working directory
        let path = PathBuf::from(path);
        let path = fs::canonicalize(&path)?;

        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        let df = ctx.read_csv(path.to_str().unwrap(), options)?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }

    /// Register a DataFrame as a table that can be referenced from a SQL query
    pub fn register_table(&self, name: &str, table: &BallistaDataFrame) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .tables
            .insert(name.to_owned(), table.to_logical_plan());
        Ok(())
    }

    pub fn register_csv(&self, name: &str, path: &str, options: CsvReadOptions) -> Result<()> {
        let df = self.read_csv(path, options)?;
        self.register_table(name, &df)
    }

    pub fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        let df = self.read_parquet(path)?;
        self.register_table(name, &df)
    }

    /// Create a DataFrame from a SQL statement
    pub fn sql(&self, sql: &str) -> Result<BallistaDataFrame> {
        // use local DataFusion context for now but later this might call the scheduler
        let mut ctx = ExecutionContext::new();
        // register tables
        let state = self.state.lock().unwrap();
        for (name, plan) in &state.tables {
            let plan = ctx.optimize(plan)?;
            let execution_plan = ctx.create_physical_plan(&plan)?;
            ctx.register_table(name, Box::new(DFTableAdapter::new(plan, execution_plan)))
        }
        let df = ctx.sql(sql)?;
        Ok(BallistaDataFrame::from(self.state.clone(), df))
    }
}

/// This ugly adapter is needed because we use DataFusion's logical plan when building queries
/// and when we register tables with DataFusion's `ExecutionContext` we need to provide a
/// TableProvider which is effectively a wrapper around a physical plan. We need to be able to
/// register tables so that we can create logical plans from SQL statements that reference these
/// tables.
pub(crate) struct DFTableAdapter {
    /// DataFusion logical plan
    pub logical_plan: LogicalPlan,
    /// DataFusion execution plan
    plan: Arc<dyn ExecutionPlan>,
}

impl DFTableAdapter {
    fn new(logical_plan: LogicalPlan, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { logical_plan, plan }
    }
}

impl TableProvider for DFTableAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self.plan.clone())
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
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

    pub async fn collect(&self) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let (host, port) = {
            let state = self.state.lock().unwrap();

            match &state.cluster_meta {
                ClusterMeta::Direct { host, port, .. } => (host.to_owned(), *port),
            }
        };

        info!("Connecting to Ballista executor at {}:{}", host, port);

        let plan = self.df.to_logical_plan();

        //TODO change this to test distributed execution
        let distributed = false;

        if distributed {
            // TODO this logic needs to move into the scheduler so that the scheduler
            // orchestrates the distributed execution
            debug!("Executing plan using distributed planner: {:?}", plan);
            // TODO get list of executors from the scheduler
            let executors = vec![ExecutorMeta {
                id: "TBD".to_string(),
                host: host.to_string(),
                port: port as u16,
            }];
            let mut planner = DistributedPlanner::new(executors)?;
            planner.collect(&plan).await
        } else {
            debug!("Sending logical plan to executor: {:?}", plan);
            let mut client = BallistaClient::try_new(&host, port).await?;
            client
                .execute_action(&Action::InteractiveQuery {
                    plan,
                    settings: Default::default(),
                })
                .await
        }
    }

    pub fn select_columns(&self, columns: &[&str]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .select_columns(columns)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn select(&self, expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.select(expr).map_err(BallistaError::from)?,
        ))
    }

    pub fn filter(&self, expr: Expr) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.filter(expr).map_err(BallistaError::from)?,
        ))
    }

    pub fn aggregate(&self, group_expr: &[Expr], aggr_expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .aggregate(group_expr, aggr_expr)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn limit(&self, n: usize) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.limit(n).map_err(BallistaError::from)?,
        ))
    }

    pub fn sort(&self, expr: &[Expr]) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.sort(expr).map_err(BallistaError::from)?,
        ))
    }

    // TODO lifetime issue
    // pub fn join(&self, right: Arc<dyn DataFrame>, join_type: JoinType, left_cols: &[&str], right_cols: &[&str]) ->
    // Result<BallistaDataFrame> {     Ok(Self::from(self.state.clone(), self.df.join(right, join_type, &left_cols,
    // &right_cols).map_err(BallistaError::from)?)) }

    pub fn repartition(&self, partitioning_scheme: Partitioning) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df
                .repartition(partitioning_scheme)
                .map_err(BallistaError::from)?,
        ))
    }

    pub fn schema(&self) -> &DFSchema {
        self.df.schema()
    }

    pub fn to_logical_plan(&self) -> LogicalPlan {
        self.df.to_logical_plan()
    }

    pub fn explain(&self, verbose: bool) -> Result<BallistaDataFrame> {
        Ok(Self::from(
            self.state.clone(),
            self.df.explain(verbose).map_err(BallistaError::from)?,
        ))
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
