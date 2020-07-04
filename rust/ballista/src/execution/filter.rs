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
use crate::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan, PhysicalPlan};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct FilterExec {
    child: Rc<PhysicalPlan>,
}

impl ExecutionPlan for FilterExec {
    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream> {
        unimplemented!()
    }
}
