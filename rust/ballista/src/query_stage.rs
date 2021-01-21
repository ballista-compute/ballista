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

use std::{any::Any, sync::Arc};

use crate::{client::BallistaClient,
            serde::scheduler::{Action, QueryStageTask}};

use crate::memory_stream::MemoryStream;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{error::{DataFusionError, Result},
                 physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream}};
use futures::StreamExt;
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
    child:    Arc<dyn ExecutionPlan>,
}

impl QueryStageExec {
    /// Create a new ShuffleHashJoinExec and validate that both inputs are using hash partitioning
    /// with compatible expressions

    pub fn try_new(job_uuid: Uuid, stage_id: usize, child: Arc<dyn ExecutionPlan>) -> Result<Self> {

        //TODO add some validation to make sure the plan is supported for distributed execution
        Ok(Self { job_uuid, stage_id, child })
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

    fn with_new_children(&self, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {

        Err(DataFusionError::Plan("Ballista QueryStageExec does not support with_new_children()".to_owned()))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {

        assert_eq!(0, partition);

        let mut batches = vec![];

        for child_partition in 0..self.child.output_partitioning().partition_count() {

            let action = Action::ExecuteQueryStage(QueryStageTask {
                job_uuid:          self.job_uuid,
                stage_id:          self.stage_id,
                partition_id:      child_partition,
                plan:              self.child.clone(),
                shuffle_locations: Default::default(),
            });

            //TODO remove hard-coded executor connection details and use cluster
            // manager (etcd or k8s) to discover executors

            // TODO could we distribute the tasks via etcd rather than sending them directly to
            // executors? See https://github.com/ballista-compute/ballista/issues/382

            let mut client = BallistaClient::try_new("localhost", 8000)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

            let mut partition_metadata = client
                .execute_action(&action)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

            //TODO we should stream rather than load into memory
            while let Some(result) = partition_metadata.next().await {

                let batch = result?;

                batches.push(batch);
            }
        }

        let schema = batches[0].schema();

        Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
    }
}
