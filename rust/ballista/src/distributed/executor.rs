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

use crate::distributed::scheduler::Task;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::ColumnarBatch;

use async_trait::async_trait;

#[async_trait]
pub trait Executor {
    async fn execute_task(&self, task: &Task) -> Result<String>;
    fn collect(&self, result_id: &str) -> Result<Vec<ColumnarBatch>>;
}

pub struct ExecutorImpl {
    results: Arc<Mutex<HashMap<String, Vec<ColumnarBatch>>>>,
}

impl ExecutorImpl {
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for ExecutorImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for ExecutorImpl {
    async fn execute_task(&self, task: &Task) -> Result<String> {
        // execute the query
        let stream = task.plan.as_execution_plan().execute(task.partition_id)?;

        // fetch the results
        let mut results = vec![];
        while let Some(batch) = stream.next().await? {
            results.push(batch);
        }

        // store the results
        let key = task.key();
        let mut map = self.results.lock().unwrap();
        map.insert(key.clone(), results);

        // return the result id
        Ok(key)
    }

    fn collect(&self, result_id: &str) -> Result<Vec<ColumnarBatch>> {
        let map = self.results.lock().unwrap();
        match map.get(result_id) {
            Some(result) => Ok(result.clone()),
            _ => Err(ballista_error("no results")),
        }
    }
}
