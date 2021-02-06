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

//! ShuffleReaderExec reads partitions that have already been materialized by an executor.
//!
//! This operator is EXPERIMENTAL and still under development

use std::any::Any;
use std::sync::Arc;

use crate::client::BallistaClient;
use crate::memory_stream::MemoryStream;
use crate::scheduler::planner::PartitionLocation;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use log::info;

/// QueryStageExec executes a subset of a query plan and returns a data set containing statistics
/// about the data.
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    // The query stage that is responsible for producing the shuffle partitions that
    // this operator will read
    partition_meta: Vec<PartitionLocation>,
    schema: SchemaRef,
}

impl ShuffleReaderExec {
    /// Create a new query stage
    pub fn try_new(partition_meta: Vec<PartitionLocation>, schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            partition_meta,
            schema,
        })
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // The output of this operator is a single partition containing metadata
        Partitioning::UnknownPartitioning(self.partition_meta.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        info!("ShuffleReaderExec::execute({})", partition);

        //TODO remove hard-coded executor connection details and use scheduler to discover
        // executors, but for now assume a single executor is running on the default port
        let mut client = BallistaClient::try_new("localhost", 50051)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        let partition_location = &self.partition_meta[partition];

        let batches = client
            .fetch_partition(
                &partition_location.partition_id.job_uuid,
                partition_location.partition_id.stage_id,
                partition,
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.schema(),
            None,
        )?))
    }
}
