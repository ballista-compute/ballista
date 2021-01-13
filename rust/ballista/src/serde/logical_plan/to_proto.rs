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
//! buffer format, allowing DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::convert::TryInto;

use crate::serde::{empty_expr_node, empty_logical_plan_node, protobuf, BallistaError};

use arrow::datatypes::{DataType, Schema};
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::CsvFile;
use datafusion::logical_plan::{Expr, JoinType, LogicalPlan};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::scalar::ScalarValue;

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::TableScan {
                table_name,
                source,
                projected_schema,
                filters,
                ..
            } => {
                let schema = source.schema();
                let source = source.as_any();
                let columns = projected_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect();
                let projection = Some(protobuf::ProjectionColumns { columns });
                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let mut node = empty_logical_plan_node();

                if let Some(parquet) = source.downcast_ref::<ParquetTable>() {
                    node.parquet_scan = Some(protobuf::ParquetTableScanNode {
                        table_name: table_name.to_owned(),
                        path: parquet.path().to_owned(),
                        projection,
                        schema: Some(schema),
                        filters,
                    });
                    Ok(node)
                } else if let Some(csv) = source.downcast_ref::<CsvFile>() {
                    node.csv_scan = Some(protobuf::CsvTableScanNode {
                        table_name: table_name.to_owned(),
                        path: csv.path().to_owned(),
                        projection,
                        schema: Some(schema),
                        has_header: csv.has_header(),
                        delimiter: csv.delimiter().to_string(),
                        file_extension: csv.file_extension().to_string(),
                        filters,
                    });
                    Ok(node)
                } else {
                    Err(BallistaError::General(format!(
                        "logical plan to_proto unsupported table provider {:?}",
                        source
                    )))
                }
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
            LogicalPlan::Filter { predicate, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.selection = Some(protobuf::SelectionNode {
                    expr: Some(predicate.try_into()?),
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
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                ..
            } => {
                let left: protobuf::LogicalPlanNode = left.as_ref().try_into()?;
                let right: protobuf::LogicalPlanNode = right.as_ref().try_into()?;
                let join_type = match join_type {
                    JoinType::Inner => protobuf::JoinType::Inner,
                    JoinType::Left => protobuf::JoinType::Left,
                    JoinType::Right => protobuf::JoinType::Right,
                };
                let left_join_column = on.iter().map(|on| on.0.to_owned()).collect();
                let right_join_column = on.iter().map(|on| on.1.to_owned()).collect();
                let mut node = empty_logical_plan_node();
                node.join = Some(Box::new(protobuf::JoinNode {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    join_type: join_type.into(),
                    left_join_column,
                    right_join_column,
                }));
                Ok(node)
            }
            LogicalPlan::Limit { input, n } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.limit = Some(protobuf::LimitNode { limit: *n as u32 });
                Ok(node)
            }
            LogicalPlan::Sort { input, expr } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                                                            .iter()
                                                            .map(|expr| expr.try_into())
                                                            .collect::<Result<Vec<_>, BallistaError>>()?;
                node.sort = Some(protobuf::SortNode{expr: selection_expr });
                Ok(node)
            },
            LogicalPlan::Repartition { .. } => unimplemented!(),
            LogicalPlan::EmptyRelation { .. } => unimplemented!(),
            LogicalPlan::CreateExternalTable { .. } => unimplemented!(),
            LogicalPlan::Explain { .. } => unimplemented!(),
            LogicalPlan::Extension { .. } => unimplemented!(),
            // _ => Err(BallistaError::General(format!(
            //     "logical plan to_proto {:?}",
            //     self
            // ))),
        }
    }
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            Expr::Column(name) => {
                let mut expr = empty_expr_node();
                expr.has_column_name = true;
                expr.column_name = name.clone();
                Ok(expr)
            }
            Expr::Alias(expr, alias) => {
                let mut expr_node = empty_expr_node();
                expr_node.alias = Some(Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                }));
                Ok(expr_node)
            }
            Expr::Literal(value) => match value {
                ScalarValue::Utf8(s) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_string = true;
                    expr.literal_string = s.as_ref().unwrap().to_owned(); //TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Int8(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_i8 = true;
                    expr.literal_int = n.unwrap() as i64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Int16(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_i16 = true;
                    expr.literal_int = n.unwrap() as i64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Int32(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_i32 = true;
                    expr.literal_int = n.unwrap() as i64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Int64(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_i64 = true;
                    expr.literal_int = n.unwrap() as i64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::UInt8(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_u8 = true;
                    expr.literal_uint = n.unwrap() as u64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::UInt16(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_u16 = true;
                    expr.literal_uint = n.unwrap() as u64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::UInt32(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_u32 = true;
                    expr.literal_uint = n.unwrap() as u64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::UInt64(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_u64 = true;
                    expr.literal_uint = n.unwrap() as u64; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Float32(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_f32 = true;
                    expr.literal_f32 = n.unwrap() as f32; // TODO remove unwrap
                    Ok(expr)
                }
                ScalarValue::Float64(n) => {
                    let mut expr = empty_expr_node();
                    expr.has_literal_f64 = true;
                    expr.literal_f64 = n.unwrap() as f64; // TODO remove unwrap
                    Ok(expr)
                }
                other => Err(BallistaError::General(format!(
                    "to_proto unsupported scalar value {:?}",
                    other
                ))),
            },
            Expr::BinaryExpr { left, op, right } => {
                let mut expr = empty_expr_node();
                expr.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                }));
                Ok(expr)
            }
            Expr::AggregateFunction {
                ref fun, ref args, ..
            } => {
                let mut expr = empty_expr_node();

                let aggr_function = match fun {
                    AggregateFunction::Min => protobuf::AggregateFunction::Min,
                    AggregateFunction::Max => protobuf::AggregateFunction::Max,
                    AggregateFunction::Sum => protobuf::AggregateFunction::Sum,
                    AggregateFunction::Avg => protobuf::AggregateFunction::Avg,
                    AggregateFunction::Count => protobuf::AggregateFunction::Count,
                };

                let arg = &args[0];
                expr.aggregate_expr = Some(Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                }));
                Ok(expr)
            }
            Expr::ScalarVariable(_) => unimplemented!(),
            Expr::ScalarFunction { .. } => unimplemented!(),
            Expr::ScalarUDF { .. } => unimplemented!(),
            Expr::AggregateUDF { .. } => unimplemented!(),
            Expr::Not(_) => unimplemented!(),
            Expr::IsNull(_) => unimplemented!(),
            Expr::IsNotNull(_) => unimplemented!(),
            Expr::Between { .. } => unimplemented!(),
            Expr::Negative(_) => unimplemented!(),
            Expr::Case { .. } => unimplemented!(),
            Expr::Cast { .. } => unimplemented!(),
            Expr::Sort { .. } => unimplemented!(),
            Expr::InList { .. } => unimplemented!(),
            Expr::Wildcard => unimplemented!(),
            // _ => Err(BallistaError::General(format!(
            //     "logical expr to_proto {:?}",
            //     self
            // ))),
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
                    proto.map(|arrow_type| protobuf::Field {
                        name: field.name().to_owned(),
                        arrow_type: arrow_type.into(),
                        nullable: field.is_nullable(),
                        children: vec![],
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
