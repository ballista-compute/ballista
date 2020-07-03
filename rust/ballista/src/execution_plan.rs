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

//! Ballista Execution Plan (Experimental).
//!
//! The execution plan is (will be) generated from the physical plan and there may be multiple
//! implementations for these traits e.g. interpreted versus code-generated and CPU vs GPU.

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::LogicalPlan;
use crate::physical_plan::{Partitioning, SortOrder};

/// Base trait for all operators
pub trait ExecutionPlan {
    /// Specifies how data is partitioned across different nodes in the cluster
    fn output_partitioning(&self) -> Partitioning;
    /// Specifies how data is ordered in each partition
    fn output_ordering(&self) -> Vec<SortOrder>;
    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_ordering(&self) -> Vec<Vec<SortOrder>>;
    /// Runs this query returning a stream of colymnar batches
    fn execute(&self); //TODO decide on return type to represent stream of record batches
    /// Runs this query returning the full results
    fn execute_collect(&self) -> Vec<ColumnarBatch>;
    /// Runs this query returning the first `n` rows
    fn execute_take(&self, n: usize) -> Vec<ColumnarBatch>;
    /// Runs this query returning the last `n` rows
    fn execute_tail(&self, n: usize) -> Vec<ColumnarBatch>;
    /// Returns the children of this operator
    fn children(&self) -> Vec<Box<dyn ExecutionPlan>>;
}

pub trait UnaryExec: ExecutionPlan {
    fn child(&self) -> Box<dyn ExecutionPlan>;

    fn children(&self) -> Vec<Box<dyn ExecutionPlan>> {
        vec![self.child()]
    }
}

pub trait BinaryExec: ExecutionPlan {
    fn left(&self) -> Box<dyn ExecutionPlan>;
    fn right(&self) -> Box<dyn ExecutionPlan>;

    fn children(&self) -> Vec<Box<dyn ExecutionPlan>> {
        vec![self.left(), self.right()]
    }
}

/// Batch of columnar data
pub struct ColumnarBatch {
    record_batch: RecordBatch,
}
