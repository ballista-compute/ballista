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

//! Core executor logic for executing queries and storing results in memory.

use std::sync::Arc;

use crate::error::Result;
use crate::scheduler::SchedulerClient;

use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::collect;
use log::{debug, info};
use uuid::Uuid;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub(crate) host: String,
    pub(crate) port: usize,
    pub(crate) concurrent_tasks: usize,
}

impl ExecutorConfig {
    pub fn new(host: &str, port: usize, concurrent_tasks: usize) -> Self {
        Self {
            host: host.to_owned(),
            port,
            concurrent_tasks,
        }
    }
}

#[allow(dead_code)]
pub struct BallistaExecutor {
    scheduler: Arc<dyn SchedulerClient>,
}

impl BallistaExecutor {
    pub fn new(_uuid: &Uuid, _config: ExecutorConfig, scheduler: Arc<dyn SchedulerClient>) -> Self {
        Self { scheduler }
    }

    pub async fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        info!("Running interactive query");
        debug!("Logical plan: {:?}", plan);
        // execute with DataFusion for now until distributed execution is in place
        let ctx = ExecutionContext::new();

        // create the query plan
        let plan = ctx.optimize(&plan).and_then(|plan| {
            debug!("Optimized logical plan: {:?}", plan);
            ctx.create_physical_plan(&plan)
        })?;

        debug!("Physical plan: {:?}", plan);
        // execute the query
        collect(plan.clone()).await.map_err(|e| e.into())
    }
}
