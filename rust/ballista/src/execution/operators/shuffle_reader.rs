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

use std::sync::Arc;

use crate::arrow::datatypes::Schema;
use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan};

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    // query stage that produced the shuffle output that this reader needs to read
    pub stage_id: usize,
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    async fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream> {
        // TODO send Flight request to the executor asking for the partition(s)
        unimplemented!()
    }
}
