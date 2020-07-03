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
use crate::execution::physical_plan::{Expression, ExecutionPlan, ColumnarBatchStream};

#[allow(dead_code)]
pub struct HashAggregateExec {
    group_expr: Vec<Rc<dyn Expression>>,
    aggr_expr: Vec<Rc<dyn Expression>>,
    child: Box<Rc<dyn ExecutionPlan>>,
}

impl ExecutionPlan for HashAggregateExec {

    fn execute(&self) -> Result<ColumnarBatchStream> {
        let _input = self.child.execute()?;

        unimplemented!()
    }
}