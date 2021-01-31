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

//! QueryStageExec executes a subset of a query plan and returns a data set containing statistics
//! about the data.
//!
//! This operator is EXPERIMENTAL and still under development

use std::any::Any;
use std::sync::Arc;

use crate::client::BallistaClient;
use crate::memory_stream::MemoryStream;

use arrow::array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use log::debug;
use uuid::Uuid;

/// QueryStageExec executes a subset of a query plan and returns a data set containing statistics
/// about the data.
#[derive(Debug, Clone)]
pub struct QueryStageExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_uuid: Uuid,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    child: Arc<dyn ExecutionPlan>,
}

impl QueryStageExec {
    /// Create a new query stage
    pub fn try_new(job_uuid: Uuid, stage_id: usize, child: Arc<dyn ExecutionPlan>) -> Result<Self> {
        //TODO add some validation to make sure the plan is supported for distributed execution
        Ok(Self {
            job_uuid,
            stage_id,
            child,
        })
    }
}

#[async_trait]
impl ExecutionPlan for QueryStageExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // The output of this operator is a single partition containing metadata
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // This operator is a special case and is generally seen as a leaf node in an
        // execution plan
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista QueryStageExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        let partition_count = self.child.output_partitioning().partition_count();

        let mut partition_id = array::UInt16Builder::new(partition_count);
        let mut partition_path = array::StringBuilder::new(partition_count);

        // TODO make this concurrent by executing all partitions at once instead of one at a time

        for child_partition in 0..partition_count {
            //TODO remove hard-coded executor connection details and use scheduler to discover
            // executors, but for now assume a single executor is running on the default port
            let mut client = BallistaClient::try_new("localhost", 50051)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

            let stage_id = &self.stage_id;
            let partition_metadata = client
                .execute_partition(
                    self.job_uuid,
                    *stage_id,
                    child_partition,
                    self.child.clone(),
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

            debug!(
                "Partition {} metadata: {:?}",
                child_partition, partition_metadata
            );

            partition_id.append_value(child_partition as u16)?;
            partition_path.append_value(partition_metadata.path())?;
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("partition_id", DataType::UInt16, false),
            // Field::new("executor_uuid", DataType::Utf8, false),
            // Field::new("row_count", DataType::UInt32, false),
            Field::new("path", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(partition_id.finish()),
                Arc::new(partition_path.finish()),
            ],
        )?;

        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
}
