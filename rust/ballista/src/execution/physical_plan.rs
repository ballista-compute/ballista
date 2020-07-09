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

use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::arrow::array::ArrayRef;
use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::Expr;
use crate::datafusion::logicalplan::ScalarValue;
use crate::error::{ballista_error, Result};
use crate::execution::expressions::simple::ColumnReference;
use crate::execution::filter::FilterExec;
use crate::execution::hash_aggregate::HashAggregateExec;
use crate::execution::parquet_scan::ParquetScanExec;
use crate::execution::projection::ProjectionExec;
use crate::execution::shuffle_exchange::ShuffleExchangeExec;
use crate::execution::shuffle_reader::ShuffleReaderExec;
use crate::execution::shuffled_hash_join::ShuffledHashJoinExec;

use crate::execution::expressions::aggregate::Max;
use async_trait::async_trait;
use std::fmt::Debug;

/// Stream of columnar batches using futures
pub type ColumnarBatchStream = Arc<dyn ColumnarBatchIter>;

/// Async iterator over a stream of columnar batches
#[async_trait]
pub trait ColumnarBatchIter: Sync + Send {
    /// Get the schema for the batches produced by this iterator.
    fn schema(&self) -> Arc<Schema>;

    /// Get the next batch from the stream, or None if the stream has ended
    async fn next(&self) -> Result<Option<ColumnarBatch>>;

    /// Notify the iterator that no more results will be fetched, so that resources
    /// can be freed immediately.
    async fn close(&self) {}
}

/// Base trait for all operators
pub trait ExecutionPlan {
    /// Specified the output schema of this operator.
    fn schema(&self) -> Arc<Schema>;

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
    fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream>;
}

pub trait Expression: Send + Sync + Debug {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a ColumnarBatch to produce a scalar or columnar result.
    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue>;
    /// Generate schema Field type for this expression
    fn to_schema_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

/// Aggregate expression that can be evaluated against a RecordBatch
pub trait AggregateExpr: Send + Sync + Debug {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate the expression being aggregated
    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue>;
    /// Create an accumulator for this aggregate expression
    fn create_accumulator(&self) -> Arc<Mutex<dyn Accumulator>>;
    /// Create an aggregate expression for combining the results of accumulators from partitions.
    /// For example, to combine the results of a parallel SUM we just need to do another SUM, but
    /// to combine the results of parallel COUNT we would also use SUM.
    fn create_reducer(&self, column_index: usize) -> Arc<dyn AggregateExpr>;
    /// Generate schema Field type for this expression
    fn to_schema_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

/// Aggregate accumulator
pub trait Accumulator: Send + Sync {
    /// Update the accumulator based on a columnar value
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()>;
    /// Get the final value for the accumulator
    fn get_value(&self) -> Result<Option<ScalarValue>>;
}

pub type MaybeColumnarBatch = Result<Option<ColumnarBatch>>;

/// Batch of columnar data.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ColumnarBatch {
    columns: Vec<ColumnarValue>,
}

impl ColumnarBatch {
    pub fn from_arrow(batch: &RecordBatch) -> Self {
        let columns = batch
            .columns()
            .iter()
            .map(|c| ColumnarValue::Columnar(c.clone()))
            .collect();
        Self { columns }
    }

    pub fn from_values(values: &[ColumnarValue]) -> Self {
        Self {
            columns: values.to_vec(),
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].len()
    }

    pub fn column(&self, index: usize) -> &ColumnarValue {
        &self.columns[index]
    }
}

/// A columnar value can either be a scalar value or an Arrow array.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ColumnarValue {
    Scalar(Option<ScalarValue>, usize),
    Columnar(ArrayRef),
}

impl ColumnarValue {
    pub fn len(&self) -> usize {
        match self {
            ColumnarValue::Scalar(_, n) => *n,
            ColumnarValue::Columnar(array) => array.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Enumeration wrapping physical plan structs so that they can be represented in a tree easily
/// and processed using pattern matching
#[derive(Clone)]
pub enum PhysicalPlan {
    /// Projection.
    Projection(Rc<ProjectionExec>),
    /// Filter a.k.a predicate.
    Filter(Rc<FilterExec>),
    /// Hash aggregate
    HashAggregate(Rc<HashAggregateExec>),
    /// Performs a hash join of two child relations by first shuffling the data using the join keys.
    ShuffledHashJoin(ShuffledHashJoinExec),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(Rc<ShuffleExchangeExec>),
    /// Reads results from a ShuffleExchange
    ShuffleReader(Rc<ShuffleReaderExec>),
    /// Scans a partitioned data source
    ParquetScan(Rc<ParquetScanExec>),
}

impl PhysicalPlan {
    pub fn as_execution_plan(&self) -> Rc<dyn ExecutionPlan> {
        match self {
            Self::Projection(exec) => exec.clone(),
            Self::Filter(exec) => exec.clone(),
            Self::HashAggregate(exec) => exec.clone(),
            Self::ParquetScan(exec) => exec.clone(),
            Self::ShuffleExchange(exec) => exec.clone(),
            Self::ShuffleReader(exec) => exec.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn with_new_children(&self, new_children: Vec<Rc<PhysicalPlan>>) -> PhysicalPlan {
        match self {
            Self::HashAggregate(exec) => {
                Self::HashAggregate(Rc::new(exec.with_new_children(new_children)))
            }
            _ => unimplemented!(),
        }
    }

    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match self {
            PhysicalPlan::ParquetScan(exec) => write!(
                f,
                "ParquetScan: {:?}, partitions={}",
                exec.path,
                exec.filenames.len()
            ),
            PhysicalPlan::HashAggregate(exec) => {
                write!(
                    f,
                    "HashAggregate: mode={:?}, groupExpr={:?}, aggrExpr={:?}",
                    exec.mode, exec.group_expr, exec.aggr_expr
                )?;
                exec.child.fmt_with_indent(f, indent + 1)
            }
            PhysicalPlan::ShuffleExchange(exec) => {
                write!(f, "Shuffle: {:?}", exec.as_ref().output_partitioning())?;
                exec.as_ref().child.fmt_with_indent(f, indent + 1)
            }
            PhysicalPlan::ShuffleReader(exec) => {
                write!(f, "ShuffleReader: stage_id={}", exec.stage_id)
            }
            PhysicalPlan::Projection(_exec) => write!(f, "Projection:"),
            PhysicalPlan::Filter(_exec) => write!(f, "Filter:"),
            _ => write!(f, "???"),
        }
    }
}

impl fmt::Debug for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

#[derive(Debug, Clone)]
pub enum Distribution {
    UnspecifiedDistribution,
    SinglePartition,
    BroadcastDistribution,
    ClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>,
    },
    HashClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>,
    },
    OrderedDistribution(Vec<SortOrder>),
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
            HashPartitioning(n, _) => *n,
        }
    }
}

/// Create a physical expression from a logical expression
pub fn compile_expression(expr: &Expr, _input: &Schema) -> Result<Arc<dyn Expression>> {
    match expr {
        Expr::Column(n) => Ok(Arc::new(ColumnReference::new(*n))),
        other => Err(ballista_error(&format!(
            "Unsupported expression {:?}",
            other
        ))),
    }
}

pub fn compile_expressions(expr: &[Expr], input: &Schema) -> Result<Vec<Arc<dyn Expression>>> {
    expr.iter().map(|e| compile_expression(e, input)).collect()
}

pub fn compile_aggregate_expression(
    expr: &Expr,
    input_schema: &Schema,
) -> Result<Arc<dyn AggregateExpr>> {
    match expr {
        Expr::AggregateFunction { name, args, .. } => {
            match name.to_lowercase().as_ref() {
                // "sum" => Ok(Arc::new(Sum::new(
                //     self.create_physical_expr(&args[0], input_schema)?,
                // ))),
                // "avg" => Ok(Arc::new(Avg::new(
                //     self.create_physical_expr(&args[0], input_schema)?,
                // ))),
                "max" => Ok(Arc::new(Max::new(compile_expression(
                    &args[0],
                    input_schema,
                )?))),
                // "min" => Ok(Arc::new(Min::new(
                //     self.create_physical_expr(&args[0], input_schema)?,
                // ))),
                // "count" => Ok(Arc::new(Count::new(
                //     self.create_physical_expr(&args[0], input_schema)?,
                // ))),
                other => Err(ballista_error(&format!(
                    "Unsupported aggregate function '{}'",
                    other
                ))),
            }
        }
        other => Err(ballista_error(&format!(
            "Unsupported aggregate expression {:?}",
            other
        ))),
    }
}

pub fn compile_aggregate_expressions(
    expr: &[Expr],
    input: &Schema,
) -> Result<Vec<Arc<dyn AggregateExpr>>> {
    expr.iter()
        .map(|e| compile_aggregate_expression(e, input))
        .collect()
}
