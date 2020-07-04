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

use crate::datafusion::logicalplan::Expr;
use crate::error::Result;
use crate::execution::physical_plan::{
    AggregateMode, ColumnarBatchStream, Distribution, ExecutionPlan, Partitioning, PhysicalPlan,
};
use std::rc::Rc;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct HashAggregateExec {
    mode: AggregateMode,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    child: Rc<PhysicalPlan>,
}

impl HashAggregateExec {
    pub fn new(mode: AggregateMode, child: Rc<PhysicalPlan>) -> Self {
        Self {
            mode,
            group_expr: vec![],
            aggr_expr: vec![],
            child,
        }
    }

    pub fn with_new_children(&self, new_children: Vec<Rc<PhysicalPlan>>) -> HashAggregateExec {
        assert!(new_children.len() == 1);
        HashAggregateExec {
            mode: self.mode.clone(),
            group_expr: self.group_expr.clone(),
            aggr_expr: self.aggr_expr.clone(),
            child: new_children[0].clone(),
        }
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn output_partitioning(&self) -> Partitioning {
        match self.mode {
            AggregateMode::Partial => self.child.as_execution_plan().output_partitioning(),
            AggregateMode::Final => Partitioning::UnknownPartitioning(1),
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        match self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            AggregateMode::Final => Distribution::SinglePartition,
        }
    }

    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream> {
        //let _input = self.child.execute(partition_index)?;

        unimplemented!()
    }
}
