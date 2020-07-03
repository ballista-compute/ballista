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

use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan};

pub struct ShuffleExchangeExec {
    child: Rc<dyn ExecutionPlan>,
}

impl ExecutionPlan for ShuffleExchangeExec {
    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        let _input = self.child.execute(partition_index)?;

        /*
        for each batch {
            for each row {
                apply hash to partition expression output to determine partition number
                add row to partition (could be in memory or on disk)
            }
        }
        */

        // note that this operator doesn't return a stream of data like the others and the
        // next stage in the plan will use a ShuffleReader to read the results
        unimplemented!()
    }
}
