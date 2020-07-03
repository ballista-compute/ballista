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

//! Ballista Physical Plan (Experimental)

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::LogicalPlan;

///
#[derive(Debug, Clone)]
pub enum Action {
    Collect { plan: LogicalPlan },
    WriteCsv { plan: LogicalPlan, path: String },
    WriteParquet { plan: LogicalPlan, path: String },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Projection.
    Project(ProjectPlan),
    /// Filter a.k.a predicate.
    Filter(FilterPlan),
    /// Take the first `limit` elements of the child's single output partition.
    GlobalLimit(GlobalLimitPlan),
    /// Limit to be applied to each partition.
    LocalLimit(LocalLimitPlan),
    /// Sort on one or more sorting expressions.
    Sort(SortPlan),
    /// Hash aggregate
    HashAggregate(HashAggregatePlan),
    /// Performs a hash join of two child relations by first shuffling the data using the join keys.
    ShuffledHashJoin(ShuffledHashJoinPlan),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(ShuffleExchangePlan),
    /// Scans a partitioned data source
    FileScan(FileScanPlan),
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
}

#[derive(Debug, Clone)]
pub enum BuildSide {
    BuildLeft,
    BuildRight,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    UnknownPartitioning(usize),
    HashPartitioning(usize, Vec<Expression>),
}

#[derive(Debug, Clone)]
pub struct ProjectPlan {
    child: Box<PhysicalPlan>,
    projection: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    child: Box<PhysicalPlan>,
    filter: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct GlobalLimitPlan {
    child: Box<PhysicalPlan>,
    limit: usize,
}

#[derive(Debug, Clone)]
pub struct LocalLimitPlan {
    child: Box<PhysicalPlan>,
    limit: usize,
}

#[derive(Debug, Clone)]
pub struct FileScanPlan {
    projection: Option<Vec<usize>>,
    partition_filters: Option<Vec<Expression>>,
    data_filters: Option<Vec<Expression>>,
    output_schema: Box<Schema>,
}

#[derive(Debug, Clone)]
pub struct ShuffleExchangePlan {
    child: Box<PhysicalPlan>,
    output_partitioning: Partitioning,
}

#[derive(Debug, Clone)]
pub struct ShuffledHashJoinPlan {
    left_keys: Vec<Expression>,
    right_keys: Vec<Expression>,
    build_side: BuildSide,
    join_type: JoinType,
    left: Box<PhysicalPlan>,
    right: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct SortOrder {
    child: Box<Expression>,
    direction: SortDirection,
    null_ordering: NullOrdering,
}

#[derive(Debug, Clone)]
pub struct SortPlan {
    sort_order: Vec<SortOrder>,
    child: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct HashAggregatePlan {
    group_expr: Vec<Expression>,
    aggr_expr: Vec<Expression>,
    child: Box<PhysicalPlan>,
}

/// Physical expression
#[derive(Debug, Clone)]
pub enum Expression {
    Column(usize),
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Execution (interpreted) below this point
///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ColumnarBatch {
    /// we just wrap Arrow RecordBatch for now, but this may change later so that we can
    /// support scalar values as well as columnar results
    record_batch: RecordBatch,
}

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
