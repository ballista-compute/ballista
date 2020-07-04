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

use crate::error::{BallistaError, Result};
use crate::execution::physical_plan::{ColumnarBatch, ExecutionPlan, Partitioning, ColumnarBatchStream, PhysicalPlan};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct ProjectionExec {
    child: Rc<PhysicalPlan>,
}

impl ProjectionExec {
    pub fn new(child: Rc<PhysicalPlan>) -> Self {
        Self { child }
    }
}

impl ExecutionPlan for ProjectionExec {

    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.child.as_execution_plan().output_partitioning()
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        unimplemented!()
    }
}