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

use crate::arrow::datatypes::Schema;

#[derive(Debug, Clone)]
pub enum PhysicalPlanNode {
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
    child: Box<PhysicalPlanNode>,
    projection: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    child: Box<PhysicalPlanNode>,
    filter: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct GlobalLimitPlan {
    child: Box<PhysicalPlanNode>,
    limit: usize,
}

#[derive(Debug, Clone)]
pub struct LocalLimitPlan {
    child: Box<PhysicalPlanNode>,
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
    child: Box<PhysicalPlanNode>,
    output_partitioning: Partitioning,
}

#[derive(Debug, Clone)]
pub struct ShuffledHashJoinPlan {
    left_keys: Vec<Expression>,
    right_keys: Vec<Expression>,
    build_side: BuildSide,
    join_type: JoinType,
    left: Box<PhysicalPlanNode>,
    right: Box<PhysicalPlanNode>,
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
    child: Box<PhysicalPlanNode>,
}

#[derive(Debug, Clone)]
pub struct HashAggregatePlan {
    group_expr: Vec<Expression>,
    aggr_expr: Vec<Expression>,
    child: Box<PhysicalPlanNode>,
}

/// Physical expression
#[derive(Debug, Clone)]
pub enum Expression {
    Column(usize),
    //TODO: add all the expressions here or possibly re-use the expressions from the logical plan
}
