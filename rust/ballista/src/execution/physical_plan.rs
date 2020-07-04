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
use std::sync::Arc;

use crate::arrow::array::ArrayRef;
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::Result;
use crate::execution::hash_aggregate::HashAggregateExec;
use crate::execution::shuffle_exchange::ShuffleExchangeExec;

use futures::stream::BoxStream;
use crate::execution::projection::ProjectionExec;
use crate::execution::filter::FilterExec;
use crate::execution::shuffled_hash_join::ShuffledHashJoinExec;
use crate::execution::parquet_scan::ParquetScanExec;
use crate::datafusion::logicalplan::Expr;

/// Stream of columnar batches using futures
pub type ColumnarBatchStream = BoxStream<'static, ColumnarBatch>;

/// Base trait for all operators
pub trait ExecutionPlan {
    /// Specifies how data is partitioned across different nodes in the cluster
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
    }

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Specifies how data is ordered in each partition
    fn output_ordering(&self) -> Option<Vec<SortOrder>> {
        None
    }

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_ordering(&self) -> Option<Vec<Vec<SortOrder>>> {
        None
    }

    /// Get the children of this plan. Leaf nodes have no children. Unary nodes have a single
    /// child. Binary nodes have two children.
    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![]
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
#[derive(Clone)]
pub struct ColumnarBatch {
    columns: Vec<Arc<ColumnarValue>>,
}

impl ColumnarBatch {
    pub fn from_arrow(batch: &RecordBatch) -> Self {
        //TODO implement
        Self { columns: vec![] }
    }
}

/// A columnar value can either be a scalar value or an Arrow array.
#[allow(dead_code)]
#[derive(Clone)]
pub enum ColumnarValue {
    Scalar(ScalarValue),
    Columnar(ArrayRef),
}

/// Enumeration wrapping physical plan structs so that they can be represented in a tree easily
/// and processed using pattern matching
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Projection.
    Projection(ProjectionExec),
    /// Filter a.k.a predicate.
    Filter(FilterExec),
    /// Hash aggregate
    HashAggregate(Rc<HashAggregateExec>),
    /// Performs a hash join of two child relations by first shuffling the data using the join keys.
    ShuffledHashJoin(ShuffledHashJoinExec),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(Rc<ShuffleExchangeExec>),
    /// Scans a partitioned data source
    ParquetScan(ParquetScanExec),
}

impl PhysicalPlan {
    pub fn as_execution_plan(&self) -> Rc<dyn ExecutionPlan> {
        match self {
            Self::Projection(exec) => Rc::new(exec.clone()),
            _ => unimplemented!()
        }

    }
}

#[derive(Debug, Clone)]
pub enum Distribution {
    UnspecifiedDistribution,
    SinglePartition,
    BroadcastDistribution,
    ClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>
    },
    HashClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>
    },
    OrderedDistribution(Vec<SortOrder>)
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
pub enum AggregateMode {
    Partial,
    Final,
}

#[derive(Debug, Clone)]
pub struct SortOrder {
    child: Rc<Expr>,
    direction: SortDirection,
    null_ordering: NullOrdering,
}

#[derive(Debug, Clone)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    UnknownPartitioning(usize),
    HashPartitioning(usize, Vec<Rc<Expr>>),
}

impl Partitioning {
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            UnknownPartitioning(n) => *n,
            HashPartitioning(n, _) => *n
        }
    }
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
