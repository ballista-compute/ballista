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

use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan};
use arrow::datatypes::Schema;
use std::sync::Arc;
use crate::execution::shuffle_manager::ShuffleManager;

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// query stage that produced the shuffle output that this reader needs to read
    pub(crate) stage_id: usize,
    /// Shuffle manager for locating shuffle data
    pub(crate) shuffle_manager: Arc<dyn ShuffleManager>
}

impl ExecutionPlan for ShuffleReaderExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {

        //TODO which partition id? map partition or reduce partition?

        let _executor_id = self.shuffle_manager.get_executor_id(self.stage_id, partition_index)?;

        // TODO send Flight request to the executor asking for the partition(s)

        unimplemented!()
    }
}
