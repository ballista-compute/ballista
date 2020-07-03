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

use futures::stream::BoxStream;

use crate::arrow::array::ArrayRef;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::Result;
use crate::execution::physical_plan::{Partitioning, SortOrder};

/// Stream of columnar batches using futures
pub type ColumnarBatchStream = BoxStream<'static, ColumnarBatch>;

/// Base trait for all operators
pub trait ExecutionPlan {
    /// Specifies how data is partitioned across different nodes in the cluster
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
    }

    /// Specifies how data is ordered in each partition
    fn output_ordering(&self) -> Option<Vec<SortOrder>> {
        None
    }

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_ordering(&self) -> Option<Vec<Vec<SortOrder>>> {
        None
    }

    /// Placeholder for code-generation capability
    fn codegen(&self) {
        unimplemented!()
    }

    /// Runs this query returning a stream of columnar batches per output partition
    fn execute(&self) -> Result<Vec<ColumnarBatchStream>>;

}

pub trait Expression {
    /// Evaluate an expression against a ColumnarBatch to produce a scalar or columnar result.
    fn evaluate(&self, input: &ColumnarBatch);

    /// Placeholder for code-generation capability
    fn codegen(&self);
}

/// Batch of columnar data. Just a wrapper around Arrow's RecordBatch for now but may change later.
#[allow(dead_code)]
pub struct ColumnarBatch {
    columns: Vec<ColumnarValue>
}

#[allow(dead_code)]
pub enum ColumnarValue {
    Scalar(ScalarValue),
    Columnar(ArrayRef),
}

