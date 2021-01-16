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

//! The ShuffleReadExec operator requests a stream of data from another executor based on data
//! previously written to disk by the ShuffleReadExec operator.

use std::any::Any;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use tonic::codegen::Arc;

/// Shuffle write operator
#[derive(Debug)]
pub struct ShuffleReadExec {
    //TODO shuffle id(s) to fetch
    /// Schema for the output of the shuffle
    schema: SchemaRef,
}

impl ShuffleReadExec {
    /// Create a new ShuffleReadExec
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReadExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "Ballista ShuffleReadExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}
