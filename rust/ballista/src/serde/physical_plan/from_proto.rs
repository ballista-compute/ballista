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

use std::sync::Arc;
use std::{convert::TryInto, unimplemented};
use datafusion::physical_plan::hash_utils::JoinType;

use datafusion::physical_plan::{
    coalesce_batches::CoalesceBatchesExec,
    empty::EmptyExec,
    expressions::PhysicalSortExpr,
    limit::{GlobalLimitExec, LocalLimitExec},
    projection::ProjectionExec,
    hash_join::HashJoinExec,
    sort::{SortExec, SortOptions},
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use crate::error::BallistaError;
use crate::serde::{proto_error, protobuf};
use crate::{convert_box_required, convert_required};

use protobuf::physical_plan_node::PhysicalPlanType;

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
                    .map(|expr| expr.try_into().map(|e| (e, "unused".to_string())))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Scan(_) => unimplemented!(),
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
                ))) // TODO: concurrency param doesn't seem to be used in datafusion. not sure how to fill this in
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::HashAggregate(_) => unimplemented!(),
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.right)?;
                let on: Vec<(String, String)> = hashjoin.on
                    .iter()
                    .map(|col| (col.left.clone(), col.right.clone()))
                    .collect();
                let join_type = protobuf::JoinType::from_i32(hashjoin.join_type).ok_or_else(|| {
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
                Ok(Arc::new(
                    HashJoinExec::try_new(left, right, &on, &join_type)?))

            },
            PhysicalPlanType::ShuffleReader(_) => unimplemented!(),
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
                                expr: expr.try_into()?,
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

impl TryInto<Arc<dyn PhysicalExpr>> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn PhysicalExpr>, Self::Error> {
        unimplemented!()
    }
}
