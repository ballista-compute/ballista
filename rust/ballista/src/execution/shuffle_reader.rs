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
use uuid::Uuid;

struct ShuffleReader {
    partitions: Vec<ShufflePartition>,
}

#[allow(dead_code)]
struct ShufflePartition {
    executor_uuid: Uuid,
    partition_uuid: Vec<Uuid>,
}

impl ExecutionPlan for ShuffleReader {
    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        let _part = &self.partitions[partition_index];

        // TODO send Flight request to the executor asking for the partition(s)

        unimplemented!()
    }
}
