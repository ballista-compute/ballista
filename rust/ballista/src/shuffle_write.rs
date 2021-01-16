// Copyright 2021 Andy Grove
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

//! The ShuffleWriteExec operator simply executes a physical plan and streams each output partition
//! to disk so that the Shuffle Read operator can later request to stream this data in subsequent
//! stages of query execution.

use std::any::Any;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use tonic::codegen::Arc;

/// Shuffle write operator
#[derive(Debug)]
pub struct ShuffleWriteExec {
    /// The child plan to execute and write shuffle partitions for
    child: Arc<dyn ExecutionPlan>,
    /// Output path for shuffle partitions
    output_path: String,
}

impl ShuffleWriteExec {
    /// Create a new ShuffleWriteExec
    pub fn new(child: Arc<dyn ExecutionPlan>, output_path: &str) -> Self {
        Self {
            child,
            output_path: output_path.to_owned(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // The output of this operator is a single partition containing shuffle ids - it does
        // not directly output data
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "Ballista ShuffleWriteExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        assert!(partition == 0);

        // TODO use tokio to execute all input partitions in parallel

        for input_partition in 0..self.child.output_partitioning().partition_count() {
            let _stream = self.child.execute(input_partition).await?;
            let _path = format!("{}/{}", self.output_path, input_partition);
            // TODO write stream to disk in IPC format
        }

        // TODO return summary of the shuffle partitions in a single RecordBatch
        todo!()
    }
}
