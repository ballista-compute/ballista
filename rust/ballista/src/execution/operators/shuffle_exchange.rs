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

use std::rc::Rc;

use crate::arrow::datatypes::Schema;
use crate::error::Result;
use crate::execution::physical_plan::{
    ColumnarBatchStream, ExecutionPlan, Partitioning, PhysicalPlan,
};

use tonic::codegen::Arc;

#[derive(Debug, Clone)]
pub struct ShuffleExchangeExec {
    pub(crate) child: Rc<PhysicalPlan>,
    output_partitioning: Partitioning,
}

impl ShuffleExchangeExec {
    pub fn new(child: Rc<PhysicalPlan>, output_partitioning: Partitioning) -> Self {
        Self {
            child,
            output_partitioning,
        }
    }
}

impl ExecutionPlan for ShuffleExchangeExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream> {
        unimplemented!()
    }
}
