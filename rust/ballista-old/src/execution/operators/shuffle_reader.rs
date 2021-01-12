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

//! Shuffle reader.

use std::sync::Arc;

use crate::arrow::datatypes::Schema;
use crate::error::Result;
use crate::execution::physical_plan::{
    ColumnarBatchStream, ExecutionContext, ExecutionPlan, ShuffleId,
};

use crate::execution::operators::InMemoryTableScanExec;
use async_trait::async_trait;

/// ShuffleReaderExec reads one or more partitions from remote executors.
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    schema: Arc<Schema>,
    pub(crate) shuffle_id: Vec<ShuffleId>,
}

impl ShuffleReaderExec {
    pub fn new(schema: Arc<Schema>, shuffle_id: Vec<ShuffleId>) -> Self {
        Self { schema, shuffle_id }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn execute(
        &self,
        ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        //TODO read shuffles in parallel
        let mut batches = vec![];
        for shuffle_id in &self.shuffle_id {
            batches.extend(ctx.read_shuffle(&shuffle_id).await?);
        }
        let exec = InMemoryTableScanExec::new(batches);
        exec.execute(ctx.clone(), partition_index).await
    }
}
