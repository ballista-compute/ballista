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

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion physical plans to be serialized and transmitted between
//! processes.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::serde::{protobuf, BallistaError};

use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::csv::CsvExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::hash_utils::JoinType;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::{
    empty::EmptyExec,
    expressions::{BinaryExpr, Column},
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

use datafusion::physical_plan::expressions::IsNullExpr;
use protobuf::physical_plan_node::PhysicalPlanType;

impl TryInto<protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PhysicalPlanNode, Self::Error> {
        let plan = self.as_any();
        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>, Self::Error>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    protobuf::ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(
                    protobuf::FilterExecNode {
                        input: Some(Box::new(input)),
                        expr: Some(exec.predicate().clone().try_into()?),
                    },
                ))),
            })
        // } else if let Some(exec) = plan.downcast_ref::<HashAggregateExec>() {
        //     let _input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
        //     //         node.hash_aggregate = Some(protobuf::HashAggregateExecNode {
        //     //             mode: match exec.mode {
        //     //                 AggregateMode::Partial => protobuf::AggregateMode::Partial,
        //     //                 AggregateMode::Final => protobuf::AggregateMode::Final,
        //     //                 AggregateMode::Complete => protobuf::AggregateMode::Complete,
        //     //             }
        //     //                 .into(),
        //     //             group_expr: exec
        //     //                 .group_expr
        //     //                 .iter()
        //     //                 .map(|expr| expr.try_into())
        //     //                 .collect::<Result<Vec<_>, BallistaError>>()?,
        //     //             aggr_expr: exec
        //     //                 .aggr_expr
        //     //                 .iter()
        //     //                 .map(|expr| expr.try_into())
        //     //                 .collect::<Result<Vec<_>, BallistaError>>()?,
        //     //         });
        //     Ok(protobuf::PhysicalPlanNode {
        //         physical_plan_type: None,
        //     })
        } else if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            let input: protobuf::PhysicalPlanNode = limit.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GlobalLimit(Box::new(
                    protobuf::GlobalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            let input: protobuf::PhysicalPlanNode = limit.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                    protobuf::LocalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
            let left: protobuf::PhysicalPlanNode = exec.left().to_owned().try_into()?;
            let right: protobuf::PhysicalPlanNode = exec.right().to_owned().try_into()?;
            let on: Vec<protobuf::JoinOn> = exec
                .on()
                .iter()
                .map(|tuple| protobuf::JoinOn {
                    left: tuple.0.to_owned(),
                    right: tuple.1.to_owned(),
                })
                .collect();
            let join_type = match exec.join_type() {
                JoinType::Inner => protobuf::JoinType::Inner,
                JoinType::Left => protobuf::JoinType::Left,
                JoinType::Right => protobuf::JoinType::Right,
            };
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(
                    protobuf::HashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                    },
                ))),
            })
        } else if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Empty(protobuf::EmptyExecNode {
                    produce_one_row: empty.produce_one_row(),
                    schema: Some(schema),
                })),
            })
        } else if let Some(coalesce_batches) = plan.downcast_ref::<CoalesceBatchesExec>() {
            let input: protobuf::PhysicalPlanNode =
                coalesce_batches.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                    protobuf::CoalesceBatchesExecNode {
                        input: Some(Box::new(input)),
                        target_batch_size: coalesce_batches.target_batch_size() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<CsvExec>() {
            let delimiter = [*exec.delimiter().unwrap()];
            let delimiter = std::str::from_utf8(&delimiter)
                .map_err(|_| BallistaError::General("Invalid CSV delimiter".to_owned()))?;

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CsvScan(protobuf::CsvScanExecNode {
                    path: exec.path().to_owned(),
                    filename: exec.filenames().to_vec(),
                    projection: exec
                        .projection()
                        .unwrap()
                        .iter()
                        .map(|n| *n as u32)
                        .collect(),
                    file_extension: exec.file_extension().to_owned(),
                    schema: Some(exec.file_schema().as_ref().try_into()?),
                    has_header: exec.has_header(),
                    delimiter: delimiter.to_string(),
                    batch_size: 32768,
                })),
            })
        } else if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            let filenames = exec
                .partitions()
                .iter()
                .flat_map(|part| part.filenames().to_owned())
                .collect();
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        filename: filenames,
                        projection: exec
                            .projection()
                            .as_ref()
                            .iter()
                            .map(|n| *n as u32)
                            .collect(),
                    },
                )),
            })
        //     PhysicalPlan::ShuffleReader(exec) => {
        //         let mut node = empty_physical_plan_node();
        //
        //         let shuffle_id: Vec<protobuf::ShuffleId> = exec
        //             .shuffle_id
        //             .iter()
        //             .map(|s| s.try_into())
        //             .collect::<Result<_, _>>()?;
        //
        //         node.shuffle_reader = Some(protobuf::ShuffleReaderExecNode {
        //             schema: Some(exec.schema().as_ref().try_into()?),
        //             shuffle_id,
        //         });
        //         Ok(node)
        //     }
        } else if let Some(exec) = plan.downcast_ref::<SortExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| {
                    let sort_expr = Box::new(protobuf::SortExprNode {
                        expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                        asc: !expr.options.descending,
                        nulls_first: expr.options.nulls_first,
                    });
                    Ok(protobuf::LogicalExprNode {
                        expr_type: Some(protobuf::logical_expr_node::ExprType::Sort(sort_expr)),
                    })
                })
                .collect::<Result<Vec<_>, Self::Error>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(
                    protobuf::SortExecNode {
                        input: Some(Box::new(input)),
                        expr,
                    },
                ))),
            })
        } else {
            Err(BallistaError::General(format!(
                "physical plan to_proto unsupported plan {:?}",
                self
            )))
        }
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        let expr = value.as_any();
        if let Some(expr) = expr.downcast_ref::<Column>() {
            Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::ColumnName(
                    expr.name().to_owned(),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            let binary_expr = Box::new(protobuf::BinaryExprNode {
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
                op: format!("{:?}", expr.op()),
            });
            Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::BinaryExpr(
                    binary_expr,
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
            Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::IsNullExpr(Box::new(
                    protobuf::IsNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    },
                ))),
            })
        } else {
            Err(BallistaError::General(format!(
                "physical_plan::from_proto() unsupported expression {:?}",
                value
            )))
        }
    }
}
