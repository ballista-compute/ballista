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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::distributed::scheduler::{Task, ChannelPair};
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{ColumnarBatch, ExecutionContext, consume_stream};

use async_trait::async_trait;
use crossbeam::channel::{unbounded, Receiver, Sender};
use uuid::Uuid;

#[async_trait]
pub trait Executor {
    async fn execute_task(&self, ctx: Arc<dyn ExecutionContext>, task: Task) -> Result<()>;
    fn collect(&self, result_id: &str) -> Result<Vec<ColumnarBatch>>;
}

/// Core executor logic lives here
pub struct DefaultExecutor {
    /// Unique id
    uuid: Uuid,
    /// Execution context is required to interact with other executors in the cluster
    ctx: Arc<dyn ExecutionContext>,
    /// Local store of shuffle partitions
    results: Arc<Mutex<HashMap<String, Vec<ColumnarBatch>>>>,
}

impl DefaultExecutor {
    pub fn try_new(ctx: Arc<dyn ExecutionContext>) -> Result<Self> {

        let uuid = Uuid::new_v4();

        let (tx, rx) = unbounded();

        ctx.register(uuid, ChannelPair::new(tx, rx))?;

        Ok(Self {
            uuid,
            ctx,
            results: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}



#[async_trait]
impl Executor for DefaultExecutor {

    async fn execute_task(&self, ctx: Arc<dyn ExecutionContext>, task: Task) -> Result<()> {
        // //smol::run(async {
        //     let stream = task.plan.as_execution_plan().execute(ctx, task.partition_id).await?;
        //     let mut results = consume_stream(stream.as_ref()).await?;
        //
        //     // store the results
        //     let key = task.key();
        //     let mut map = self.results.lock().unwrap();
        //     map.insert(key.clone(), results);
        //
        //     // return the result id
        //     Ok(key)
        // //})
        Ok(())
    }

    fn collect(&self, result_id: &str) -> Result<Vec<ColumnarBatch>> {
        let map = self.results.lock().unwrap();
        match map.get(result_id) {
            Some(result) => Ok(result.clone()),
            _ => Err(ballista_error("no results")),
        }
    }
}
