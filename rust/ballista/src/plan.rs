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

use crate::arrow::datatypes::Schema;
use crate::datafusion::logicalplan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum Action {
    Collect { plan: LogicalPlan },
    WriteCsv { plan: LogicalPlan, path: String },
    WriteParquet { plan: LogicalPlan, path: String },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Projection.
    Project,
    /// Filter a.k.a predicate.
    Filter(Expression),
    /// Take the first `limit` elements of the child's single output partition.
    GlobalLimit,
    /// Limit to be applied to each partition.
    LocalLimit,
    /// Sort on one or more sorting expressions.
    Sort(SortExec),
    /// Hash aggregate
    HashAggregate(HashAggregateExec),
    /// Performs a hash join of two child relations by first shuffling the data using the join keys.
    ShuffledHashJoin(ShuffledHashJoinExec),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(ShuffleExchangeExec),
    /// Scans a partitioned data source
    FileScan(FileScanExec),
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
pub struct FileScanExec {
    projection: Option<Vec<usize>>,
    partition_filters: Option<Vec<Expression>>,
    data_filters: Option<Vec<Expression>>,
    output_schema: Box<Schema>,
}

#[derive(Debug, Clone)]
pub struct ShuffleExchangeExec {
    child: Box<PhysicalPlan>,
    output_partitioning: Partitioning,
}

#[derive(Debug, Clone)]
pub struct ShuffledHashJoinExec {
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
pub struct SortExec {
    sort_order: Vec<SortOrder>,
    child: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct HashAggregateExec {
    group_expr: Vec<Expression>,
    aggr_expr: Vec<Expression>,
    child: Box<PhysicalPlan>,
}

/// Physical expression
#[derive(Debug, Clone)]
pub enum Expression {
    Column(usize),
}
