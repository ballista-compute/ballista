// Copyright 2021 Andy Grove
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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::collections::HashMap;
use std::sync::Arc;
use std::{convert::TryInto, unimplemented};

use crate::error::BallistaError;
use crate::executor::shuffle_reader::ShuffleReaderExec;
use crate::scheduler::planner::PartitionLocation;
use crate::serde::protobuf::LogicalExprNode;
use crate::serde::{proto_error, protobuf};
use crate::{convert_box_required, convert_required};

use arrow::datatypes::{DataType, Schema};
use datafusion::execution::context::{ExecutionConfig, ExecutionContextState};
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::expressions::col;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{
    coalesce_batches::CoalesceBatchesExec,
    csv::CsvExec,
    empty::EmptyExec,
    expressions::{Avg, Column, PhysicalSortExpr},
    filter::FilterExec,
    hash_join::HashJoinExec,
    hash_utils::JoinType,
    limit::{GlobalLimitExec, LocalLimitExec},
    parquet::ParquetExec,
    projection::ProjectionExec,
    sort::{SortExec, SortOptions},
};

use datafusion::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr};
use datafusion::prelude::CsvReadOptions;

use protobuf::logical_expr_node::ExprType;
use protobuf::physical_plan_node::PhysicalPlanType;

use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};

impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(projection.input)?;
                let exprs = projection
                    .expr
                    .iter()
                    .map(|expr| {
                        compile_expr(expr, &input.schema()).map(|e| (e, "unused".to_string()))
                    })
                    // .map(|expr| expr.try_into().map(|e| (e, "unused".to_string())))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicate = compile_expr(filter.expr.as_ref().unwrap(), &input.schema())?;
                Ok(Arc::new(FilterExec::try_new(predicate, input)?))
            }
            PhysicalPlanType::CsvScan(scan) => {
                let schema = Arc::new(convert_required!(scan.schema)?);
                let options = CsvReadOptions::new()
                    .has_header(scan.has_header)
                    .file_extension(&scan.file_extension)
                    .delimiter(scan.delimiter.as_bytes()[0])
                    .schema(&schema);
                // TODO we don't care what the DataFusion batch size was because Ballista will
                // have its own configs. Hard-code for now.
                let batch_size = 32768;
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                Ok(Arc::new(CsvExec::try_new(
                    &scan.path,
                    options,
                    Some(projection),
                    batch_size,
                )?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                // TODO we don't care what the DataFusion batch size was because Ballista will
                // have its own configs. Hard-code for now.
                let batch_size = 32768;
                let max_concurrency = 8;
                let filenames: Vec<&str> = scan.filename.iter().map(|s| s.as_str()).collect();
                Ok(Arc::new(ParquetExec::try_from_files(
                    &filenames,
                    Some(projection),
                    None,
                    batch_size,
                    max_concurrency,
                )?))
            }
            PhysicalPlanType::Selection(_) => unimplemented!(),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(GlobalLimitExec::new(
                    input,
                    limit.limit as usize,
                    0,
                )))
                // TODO: concurrency param doesn't seem to be used in datafusion. not sure how to fill this in
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::HashAggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(hash_agg.input)?;
                let mode = protobuf::AggregateMode::from_i32(hash_agg.mode).ok_or_else(|| {
                    proto_error(format!(
                        "Received a HashAggregateNode message with unknown AggregateMode {}",
                        hash_agg.mode
                    ))
                })?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                };
                let group = hash_agg
                    .group_expr
                    .iter()
                    .map(|expr| {
                        compile_expr(expr, &input.schema()).map(|e| (e, "unused".to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let datafusion_planner = DefaultPhysicalPlanner::default();
                let ctx_state = ExecutionContextState {
                    datasources: Default::default(),
                    scalar_functions: Default::default(),
                    var_provider: Default::default(),
                    aggregate_functions: Default::default(),
                    config: ExecutionConfig::new(),
                };

                let agg_expr = hash_agg
                    .aggr_expr
                    .iter()
                    .map(|expr| {
                        let expr2: Expr = expr.try_into().unwrap();
                        let logical_schema: DFSchema =
                            input.schema().as_ref().clone().try_into().unwrap();
                        datafusion_planner.create_aggregate_expr(
                            &expr2,
                            &logical_schema,
                            input.schema().as_ref(),
                            &ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(HashAggregateExec::try_new(
                    agg_mode, group, agg_expr, input,
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.right)?;
                let on: Vec<(String, String)> = hashjoin
                    .on
                    .iter()
                    .map(|col| (col.left.clone(), col.right.clone()))
                    .collect();
                let join_type =
                    protobuf::JoinType::from_i32(hashjoin.join_type).ok_or_else(|| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown JoinType {}",
                            hashjoin.join_type
                        ))
                    })?;
                let join_type = match join_type {
                    protobuf::JoinType::Inner => JoinType::Inner,
                    protobuf::JoinType::Left => JoinType::Left,
                    protobuf::JoinType::Right => JoinType::Right,
                };
                Ok(Arc::new(HashJoinExec::try_new(
                    left, right, &on, &join_type,
                )?))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let schema = Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<PartitionLocation> = shuffle_reader
                    .partition_location
                    .iter()
                    .map(|p| p.clone().try_into())
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                let shuffle_reader = ShuffleReaderExec::try_new(partition_location, schema)?;
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::logical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: compile_expr(expr, &input.schema())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(BallistaError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // Update concurrency here in the future
                Ok(Arc::new(SortExec::try_new(exprs, input, 1)?))
            }
        }
    }
}

/*
impl TryInto<Arc<dyn AggregateExpr>> for protobuf::LogicalExprNode {
    type Error = BallistaError;
    fn try_into(self) -> Result<Arc<dyn AggregateExpr>, Self::Error> {
        let plan = self.expr_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            ExprType::AggregateExpr::Avg(a) => {
                let agg_fn = a.aggr_function.try_into();
                let expr = a.expr.try_into();
                Ok(Arc::new(Avg::new(expr, "".to_string(), DataType::Float64))) // TODO
            }
        }
    }
}
*/

fn compile_expr(
    expr: &protobuf::LogicalExprNode,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, BallistaError> {
    let df_planner = DefaultPhysicalPlanner::default();
    let state = ExecutionContextState {
        datasources: HashMap::new(),
        scalar_functions: HashMap::new(),
        var_provider: HashMap::new(),
        aggregate_functions: HashMap::new(),
        config: ExecutionConfig::new(),
    };
    let expr: Expr = expr.try_into()?;
    df_planner
        .create_physical_expr(&expr, schema, &state)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
}
