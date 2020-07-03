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

//! Ballista Physical Plan (Experimental).
//!
//! The physical plan is a serializable data structure describing how the plan will be executed.
//!
//! It differs from the logical plan in that it deals with specific implementations of operators
//! (e.g. SortMergeJoin versus BroadcastHashJoin) whereas the logical plan just deals with an
//! abstract concept of a join.
//!
//! The physical plan also accounts for partitioning and ordering of data between operators.

use std::rc::Rc;

use crate::arrow::array::ArrayRef;
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::Result;
use crate::execution::hash_aggregate::HashAggregateExec;

use crate::execution::shuffle_exchange::ShuffleExchangeExec;
use futures::stream::BoxStream;

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

    /// Runs this query against one partition returning a stream of columnar batches
    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream>;
}

pub trait Expression {
    /// Evaluate an expression against a ColumnarBatch to produce a scalar or columnar result.
    fn evaluate(&self, input: &ColumnarBatch);
}

/// Batch of columnar data.
#[allow(dead_code)]
pub struct ColumnarBatch {
    columns: Vec<ColumnarValue>,
}

/// A columnar value can either be a scalar value or an Arrow array.
#[allow(dead_code)]
pub enum ColumnarValue {
    Scalar(ScalarValue),
    Columnar(ArrayRef),
}

/// Enumeration wrapping physical plan structs so that they can be represented in a tree easily
/// and processed using pattern matching
#[derive(Clone)]
pub enum PhysicalPlanNode {
    // /// Projection.
    // Project(ProjectPlan),
    // /// Filter a.k.a predicate.
    // Filter(FilterPlan),
    // /// Take the first `limit` elements of the child's single output partition.
    // GlobalLimit(GlobalLimitPlan),
    // /// Limit to be applied to each partition.
    // LocalLimit(LocalLimitPlan),
    // /// Sort on one or more sorting expressions.
    // Sort(SortPlan),
    /// Hash aggregate
    HashAggregate(Rc<HashAggregateExec>),
    // /// Performs a hash join of two child relations by first shuffling the data using the join keys.
    // ShuffledHashJoin(ShuffledHashJoinPlan),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(Rc<ShuffleExchangeExec>),
    // /// Scans a partitioned data source
    // FileScan(FileScanPlan),
}

#[derive(Clone)]
pub enum JoinType {
    Inner,
}

#[derive(Clone)]
pub enum BuildSide {
    BuildLeft,
    BuildRight,
}

#[derive(Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone)]
pub struct SortOrder {
    child: Rc<dyn Expression>,
    direction: SortDirection,
    null_ordering: NullOrdering,
}

#[derive(Clone)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

#[derive(Clone)]
pub enum Partitioning {
    UnknownPartitioning(usize),
    HashPartitioning(usize, Vec<Rc<dyn Expression>>),
}

// #[derive(Clone)]
// pub struct ProjectPlan {
//     child: Box<PhysicalPlanNode>,
//     projection: Vec<Expression>,
// }
//
// #[derive(Clone)]
// pub struct FilterPlan {
//     child: Box<PhysicalPlanNode>,
//     filter: Box<Expression>,
// }
//
// #[derive(Clone)]
// pub struct GlobalLimitPlan {
//     child: Box<PhysicalPlanNode>,
//     limit: usize,
// }
//
// #[derive(Clone)]
// pub struct LocalLimitPlan {
//     child: Box<PhysicalPlanNode>,
//     limit: usize,
// }
//
// #[derive(Clone)]
// pub struct FileScanPlan {
//     projection: Option<Vec<usize>>,
//     partition_filters: Option<Vec<Expression>>,
//     data_filters: Option<Vec<Expression>>,
//     output_schema: Box<Schema>,
// }
//
// #[derive(Clone)]
// pub struct ShuffleExchangePlan {
//     child: Box<PhysicalPlanNode>,
//     output_partitioning: Partitioning,
// }
//
// #[derive(Clone)]
// pub struct ShuffledHashJoinPlan {
//     left_keys: Vec<Expression>,
//     right_keys: Vec<Expression>,
//     build_side: BuildSide,
//     join_type: JoinType,
//     left: Box<PhysicalPlanNode>,
//     right: Box<PhysicalPlanNode>,
// // }

//
// #[derive(Clone)]
// pub struct SortPlan {
//     sort_order: Vec<SortOrder>,
//     child: Box<PhysicalPlanNode>,
// }
