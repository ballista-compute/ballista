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

//! Serde code to convert from Rust data structures to protocol buffers.

use std::convert::TryInto;

use crate::arrow::datatypes::{DataType, Schema};
use crate::datafusion::logicalplan::{Expr, LogicalPlan, ScalarValue};
use crate::distributed::scheduler::ExecutionTask;
use crate::error::BallistaError;
use crate::execution::physical_plan::{Action, ExecutionPlan, ShuffleId};
use crate::execution::physical_plan::{AggregateMode, PhysicalPlan};
use crate::protobuf;

impl TryInto<protobuf::Action> for &Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::InteractiveQuery { ref plan } => {
                let plan_proto: protobuf::LogicalPlanNode = plan.try_into()?;
                Ok(protobuf::Action {
                    query: Some(plan_proto),
                    task: None,
                    fetch_shuffle: None,
                })
            }
            Action::Execute(task) => Ok(protobuf::Action {
                query: None,
                task: Some(task.try_into()?),
                fetch_shuffle: None,
            }),
            Action::FetchShuffle(shuffle_id) => Ok(protobuf::Action {
                query: None,
                task: None,
                fetch_shuffle: Some(shuffle_id.try_into()?),
            }),
        }
    }
}

impl TryInto<protobuf::Schema> for &Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Schema, Self::Error> {
        Ok(protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(|field| {
                    let proto = to_proto_arrow_type(&field.data_type());
                    proto.and_then(|arrow_type| {
                        Ok(protobuf::Field {
                            name: field.name().to_owned(),
                            arrow_type: arrow_type.into(),
                            nullable: field.is_nullable(),
                            children: vec![],
                        })
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

fn to_proto_arrow_type(dt: &DataType) -> Result<protobuf::ArrowType, BallistaError> {
    match dt {
        DataType::Int8 => Ok(protobuf::ArrowType::Int8),
        DataType::Int16 => Ok(protobuf::ArrowType::Int16),
        DataType::Int32 => Ok(protobuf::ArrowType::Int32),
        DataType::Int64 => Ok(protobuf::ArrowType::Int64),
        DataType::UInt8 => Ok(protobuf::ArrowType::Uint8),
        DataType::UInt16 => Ok(protobuf::ArrowType::Uint16),
        DataType::UInt32 => Ok(protobuf::ArrowType::Uint32),
        DataType::UInt64 => Ok(protobuf::ArrowType::Uint64),
        DataType::Float32 => Ok(protobuf::ArrowType::Float),
        DataType::Float64 => Ok(protobuf::ArrowType::Double),
        DataType::Utf8 => Ok(protobuf::ArrowType::Utf8),
        other => Err(BallistaError::General(format!(
            "Unsupported data type {:?}",
            other
        ))),
    }
}

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::CsvScan {
                path,
                schema,
                projection,
                has_header,
                ..
            } => {
                let mut node = empty_logical_plan_node();

                let projected_field_names = match projection {
                    Some(p) => p.iter().map(|i| schema.field(*i).name().clone()).collect(),
                    _ => vec![],
                };

                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                node.scan = Some(protobuf::ScanNode {
                    path: path.to_owned(),
                    projection: projected_field_names,
                    schema: Some(schema),
                    has_header: *has_header,
                    file_format: "csv".to_owned(),
                });
                Ok(node)
            }
            LogicalPlan::ParquetScan {
                path,
                schema,
                projection,
                ..
            } => {
                let mut node = empty_logical_plan_node();

                let projected_field_names = match projection {
                    Some(p) => p.iter().map(|i| schema.field(*i).name().clone()).collect(),
                    _ => vec![],
                };

                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                node.scan = Some(protobuf::ScanNode {
                    path: path.to_owned(),
                    projection: projected_field_names,
                    schema: Some(schema),
                    has_header: false,
                    file_format: "parquet".to_owned(),
                });
                Ok(node)
            }
            LogicalPlan::Projection { expr, input, .. } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.projection = Some(protobuf::ProjectionNode {
                    expr: expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            LogicalPlan::Selection { expr, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.selection = Some(protobuf::SelectionNode {
                    expr: Some(expr.try_into()?),
                });
                Ok(node)
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.aggregate = Some(protobuf::AggregateNode {
                    group_expr: group_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    aggr_expr: aggr_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            _ => Err(BallistaError::NotImplemented(format!(
                "logical plan to_proto {:?}",
                self
            ))),
        }
    }
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            Expr::Column(index) => {
                let mut expr = empty_expr_node();
                expr.has_column_index = true;
                expr.column_index = *index as u32;
                Ok(expr)
            }
            Expr::UnresolvedColumn(name) => {
                let mut expr = empty_expr_node();
                expr.has_column_name = true;
                expr.column_name = name.to_owned();
                Ok(expr)
            }
            Expr::Literal(ScalarValue::Utf8(s)) => {
                let mut expr = empty_expr_node();
                expr.has_literal_string = true;
                expr.literal_string = s.to_owned();
                Ok(expr)
            }
            Expr::BinaryExpr { left, op, right } => {
                let mut expr = empty_expr_node();
                expr.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                }));
                Ok(expr)
            }
            Expr::AggregateFunction { name, ref args, .. } => {
                let mut expr = empty_expr_node();

                let aggr_function = match name.as_str() {
                    "MIN" => Ok(protobuf::AggregateFunction::Min),
                    "MAX" => Ok(protobuf::AggregateFunction::Max),
                    "SUM" => Ok(protobuf::AggregateFunction::Sum),
                    "AVG" => Ok(protobuf::AggregateFunction::Avg),
                    "COUNT" => Ok(protobuf::AggregateFunction::Count),
                    other => Err(BallistaError::NotImplemented(format!(
                        "Aggregate function {:?}",
                        other
                    ))),
                }?;

                let arg = &args[0];
                expr.aggregate_expr = Some(Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                }));
                Ok(expr)
            }
            _ => Err(BallistaError::NotImplemented(format!(
                "logical expr to_proto {:?}",
                self
            ))),
        }
    }
}

impl TryInto<protobuf::PhysicalPlanNode> for &PhysicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PhysicalPlanNode, Self::Error> {
        match self {
            PhysicalPlan::HashAggregate(exec) => {
                let input: protobuf::PhysicalPlanNode = exec.child.as_ref().try_into()?;
                let mut node = empty_physical_plan_node();
                node.input = Some(Box::new(input));
                node.hash_aggregate = Some(protobuf::HashAggregateExecNode {
                    mode: match exec.mode {
                        AggregateMode::Partial => protobuf::AggregateMode::Partial,
                        AggregateMode::Final => protobuf::AggregateMode::Final,
                        AggregateMode::Complete => protobuf::AggregateMode::Complete,
                    }
                    .into(),
                    group_expr: exec
                        .group_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    aggr_expr: exec
                        .aggr_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            PhysicalPlan::CsvScan(exec) => {
                let mut node = empty_physical_plan_node();
                node.scan = Some(protobuf::ScanExecNode {
                    path: exec.path.clone(),
                    projection: exec
                        .projection
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|n| *n as u32)
                        .collect(),
                    file_format: "csv".to_owned(),
                    schema: Some(exec.schema().as_ref().try_into()?),
                    has_header: false,
                });
                Ok(node)
            }
            PhysicalPlan::ParquetScan(exec) => {
                let mut node = empty_physical_plan_node();
                let _schema = &exec.parquet_schema;
                // let projection: Vec<String> = match &exec.projection {
                //     Some(p) => p.iter().map(|i| schema.field(*i).name().clone()).collect(),
                //     _ => vec![],
                // };
                node.scan = Some(protobuf::ScanExecNode {
                    path: exec.path.clone(),
                    projection: exec
                        .projection
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|n| *n as u32)
                        .collect(),
                    file_format: "parquet".to_owned(),
                    schema: None,
                    has_header: false,
                });
                Ok(node)
            }
            PhysicalPlan::ShuffleReader(exec) => {
                let mut node = empty_physical_plan_node();

                let shuffle_id: Vec<protobuf::ShuffleId> = exec
                    .shuffle_id
                    .iter()
                    .map(|s| s.try_into())
                    .collect::<Result<_, _>>()?;

                node.shuffle_reader = Some(protobuf::ShuffleReaderExecNode {
                    schema: Some(exec.schema().as_ref().try_into()?),
                    shuffle_id,
                });
                Ok(node)
            }
            _ => Err(BallistaError::NotImplemented(format!(
                "physical plan to_proto {:?}",
                self
            ))),
        }
    }
}

impl TryInto<protobuf::ShuffleId> for &ShuffleId {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::ShuffleId, Self::Error> {
        Ok(protobuf::ShuffleId {
            job_uuid: self.job_uuid.to_string(),
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
        })
    }
}

impl TryInto<protobuf::Task> for &ExecutionTask {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Task, Self::Error> {
        let mut shuffle_loc = vec![];

        for (k, v) in &self.shuffle_locations {
            shuffle_loc.push(protobuf::ShuffleLocation {
                job_uuid: k.job_uuid.to_string(),
                stage_id: k.stage_id as u32,
                partition_id: k.partition_id as u32,
                executor_id: v.id.to_string(),
                executor_host: v.host.to_string(),
                executor_port: v.port as u32,
            });
        }

        let plan = &self.plan;
        Ok(protobuf::Task {
            job_uuid: self.job_uuid.to_string(),
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
            task_id: 0,
            plan: Some(plan.try_into()?),
            shuffle_loc,
        })
    }
}

/// Create an empty ExprNode
fn empty_expr_node() -> protobuf::LogicalExprNode {
    protobuf::LogicalExprNode {
        column_name: "".to_owned(),
        has_column_name: false,
        literal_string: "".to_owned(),
        has_literal_string: false,
        literal_double: 0.0,
        has_literal_double: false,
        literal_long: 0,
        has_literal_long: false,
        column_index: 0,
        has_column_index: false,
        binary_expr: None,
        aggregate_expr: None,
    }
}

/// Create an empty LogicalPlanNode
fn empty_logical_plan_node() -> protobuf::LogicalPlanNode {
    protobuf::LogicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
    }
}

/// Create an empty PhysicalPlanNode
fn empty_physical_plan_node() -> protobuf::PhysicalPlanNode {
    protobuf::PhysicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        global_limit: None,
        local_limit: None,
        shuffle_reader: None,
        hash_aggregate: None,
    }
}
