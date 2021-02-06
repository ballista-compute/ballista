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

use std::{any::Any, pin::Pin};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{error::Result, physical_plan::RecordBatchStream};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use uuid::Uuid;

/// QueryStageExec executes a subset of a query plan and returns a data set containing statistics
/// about the data.
#[derive(Debug, Clone)]
pub struct QueryStageExec {
    /// Unique ID for the job (query) that this stage is a part of
    pub(crate) job_uuid: Uuid,
    /// Unique query stage ID within the job
    pub(crate) stage_id: usize,
    /// Physical execution plan for this query stage
    pub(crate) child: Arc<dyn ExecutionPlan>,
}

impl QueryStageExec {
    /// Create a new query stage
    pub fn try_new(job_uuid: Uuid, stage_id: usize, child: Arc<dyn ExecutionPlan>) -> Result<Self> {
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
        self.child.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(QueryStageExec::try_new(
            self.job_uuid,
            self.stage_id,
            children[0].clone(),
        )?))
    }

    async fn execute(&self, partition: usize) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        self.child.execute(partition).await
    }
}
