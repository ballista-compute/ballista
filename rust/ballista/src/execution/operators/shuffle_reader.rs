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

use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan, ExecutionContext};
use crate::arrow::datatypes::Schema;
use crate::execution::physical_plan::ShuffleManager;

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// query stage that produced the shuffle output that this reader needs to read
    pub(crate) shuffle_id: String,
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    async fn execute(&self, ctx: Arc<dyn ExecutionContext>, partition_index: usize) -> Result<ColumnarBatchStream> {
        // let shuffle_manager = ctx.shuffle_manager().await;
        // Ok(shuffle_manager.read_shuffle(&self.shuffle_id).await)
        unimplemented!()
    }
}
