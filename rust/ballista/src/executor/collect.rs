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

//! The CollectExec operator retrieves results from the cluster and returns them as a single
//! vector of [RecordBatch].

use std::any::Any;
use std::sync::Arc;

use crate::memory_stream::MemoryStream;
use crate::utils;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};

#[derive(Debug, Clone)]
pub struct CollectExec {
    plan: Arc<dyn ExecutionPlan>,
}

impl CollectExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

#[async_trait]
impl ExecutionPlan for CollectExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // TODO reimplement this to use true streaming end to end rather than fetch
        // into memory and then re-stream
        assert_eq!(0, partition);
        let num_partitions = self.plan.output_partitioning().partition_count();
        let mut batches = vec![];
        for i in 0..num_partitions {
            let mut stream = self.plan.execute(i).await?;
            let partition_results = utils::collect_stream(&mut stream).await?;
            batches.extend_from_slice(&partition_results);
        }
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.schema(),
            None,
        )?))
    }
}
