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
use std::str;

use crate::context::DFTableAdapter;
use crate::serde::{protobuf, BallistaError};

use arrow::datatypes::{DataType, Schema};
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::CsvFile;
use datafusion::logical_plan::{Expr, JoinType, LogicalPlan};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::scalar::ScalarValue;
use protobuf::logical_expr_node::ExprType;
use protobuf::logical_plan_node::LogicalPlanType;

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::TableScan {
                table_name,
                source,
                filters,
                projection,
                ..
            } => {
                let schema = source.schema();

                // unwrap the DFTableAdapter to get to the real TableProvider
                let source = if let Some(adapter) = source.as_any().downcast_ref::<DFTableAdapter>()
                {
                    match &adapter.logical_plan {
                        LogicalPlan::TableScan { source, .. } => Ok(source.as_any()),
                        _ => Err(BallistaError::General(
                            "Invalid LogicalPlan::TableScan".to_owned(),
                        )),
                    }
                } else {
                    Ok(source.as_any())
                }?;

                let projection = match projection {
                    None => None,
                    Some(columns) => {
                        let column_names = columns
                            .iter()
                            .map(|i| schema.field(*i).name().to_owned())
                            .collect();
                        Some(protobuf::ProjectionColumns {
                            columns: column_names,
                        })
                    }
                };
                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                if let Some(parquet) = source.downcast_ref::<ParquetTable>() {
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ParquetScan(
                            protobuf::ParquetTableScanNode {
                                table_name: table_name.to_owned(),
                                path: parquet.path().to_owned(),
                                projection,
                                schema: Some(schema),
                                filters,
                            },
                        )),
                    })
                } else if let Some(csv) = source.downcast_ref::<CsvFile>() {
                    let delimiter = [csv.delimiter()];
                    let delimiter = str::from_utf8(&delimiter)
                        .map_err(|_| BallistaError::General("Invalid CSV delimiter".to_owned()))?;
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::CsvScan(
                            protobuf::CsvTableScanNode {
                                table_name: table_name.to_owned(),
                                path: csv.path().to_owned(),
                                projection,
                                schema: Some(schema),
                                has_header: csv.has_header(),
                                delimiter: delimiter.to_string(),
                                file_extension: csv.file_extension().to_string(),
                                filters,
                            },
                        )),
                    })
                } else {
                    Err(BallistaError::General(format!(
                        "logical plan to_proto unsupported table provider {:?}",
                        source
                    )))
                }
            }
            LogicalPlan::Projection { expr, input, .. } => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::Projection(Box::new(
                    protobuf::ProjectionNode {
                        input: Some(Box::new(input.as_ref().try_into()?)),
                        expr: expr.iter().map(|expr| expr.try_into()).collect::<Result<
                            Vec<_>,
                            BallistaError,
                        >>(
                        )?,
                    },
                ))),
            }),
            LogicalPlan::Filter { predicate, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Selection(Box::new(
                        protobuf::SelectionNode {
                            input: Some(Box::new(input)),
                            expr: Some(predicate.try_into()?),
                        },
                    ))),
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Aggregate(Box::new(
                        protobuf::AggregateNode {
                            input: Some(Box::new(input)),
                            group_expr: group_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, BallistaError>>()?,
                            aggr_expr: aggr_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, BallistaError>>()?,
                        },
                    ))),
                })
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
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Join(Box::new(protobuf::JoinNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        join_type: join_type.into(),
                        left_join_column,
                        right_join_column,
                    }))),
                })
            }
            LogicalPlan::Limit { input, n } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Limit(Box::new(
                        protobuf::LimitNode {
                            input: Some(Box::new(input)),
                            limit: *n as u32,
                        },
                    ))),
                })
            }
            LogicalPlan::Sort { input, expr } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Sort(Box::new(protobuf::SortNode {
                        input: Some(Box::new(input)),
                        expr: selection_expr,
                    }))),
                })
            }
            LogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => {
                use datafusion::logical_plan::Partitioning;
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                //Assumed common usize field was batch size
                //Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, batch_size) => {
                        PartitionMethod::Hash(protobuf::HashRepartition {
                            hash_expr: exprs.iter().map(|expr| expr.try_into()).collect::<Result<
                                Vec<_>,
                                BallistaError,
                            >>(
                            )?,
                            batch_size: *batch_size as u64,
                        })
                    }
                    Partitioning::RoundRobinBatch(batch_size) => {
                        PartitionMethod::RoundRobin(*batch_size as u64)
                    }
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Repartition(Box::new(
                        protobuf::RepartitionNode {
                            input: Some(Box::new(input)),
                            partition_method: Some(pb_partition_method),
                        },
                    ))),
                })
            }
            LogicalPlan::EmptyRelation {
                produce_one_row, ..
            } => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::EmptyRelation(
                    protobuf::EmptyRelationNode {
                        produce_one_row: *produce_one_row,
                    },
                )),
            }),
            LogicalPlan::CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                schema: df_schema,
            } => {
                use datafusion::sql::parser::FileType;
                let schema: Schema = df_schema.as_ref().clone().into();
                let pb_schema: protobuf::Schema = (&schema).try_into().map_err(|e| {
                    BallistaError::General(format!(
                        "Could not convert schema into protobuf: {:?}",
                        e
                    ))
                })?;

                let pb_file_type: protobuf::FileType = match file_type {
                    FileType::NdJson => protobuf::FileType::NdJson,
                    FileType::Parquet => protobuf::FileType::Parquet,
                    FileType::CSV => protobuf::FileType::Csv,
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CreateExternalTable(
                        protobuf::CreateExternalTableNode {
                            name: name.clone(),
                            location: location.clone(),
                            file_type: pb_file_type as i32,
                            has_header: *has_header,
                            schema: Some(pb_schema),
                        },
                    )),
                })
            }
            LogicalPlan::Explain { verbose, plan, .. } => {
                let input: protobuf::LogicalPlanNode = plan.as_ref().try_into()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Explain(Box::new(
                        protobuf::ExplainNode {
                            input: Some(Box::new(input)),
                            verbose: *verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Extension { .. } => unimplemented!(),
            // _ => Err(BallistaError::General(format!(
            //     "logical plan to_proto {:?}",
            //     self
            // ))),
        }
    }
}

fn create_proto_scalar<I, T: FnOnce(&I) -> ExprType>(
    v: &Option<I>,
    null_arrow_type: protobuf::ArrowType,
    constructor: T,
) -> ExprType {
    v.as_ref()
        .map(constructor)
        .unwrap_or(ExprType::LiteralNull(null_arrow_type as i32))
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            Expr::Column(name) => {
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::ColumnName(name.clone())),
                };
                Ok(expr)
            }
            Expr::Alias(expr, alias) => {
                let alias = Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                });
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Alias(alias)),
                };
                Ok(expr)
            }
            Expr::Literal(value) => match value {
                ScalarValue::Utf8(s) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(s, protobuf::ArrowType::Utf8, |s| {
                        ExprType::LiteralString(s.to_owned())
                    })),
                }),
                ScalarValue::Int8(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Int8, |s| {
                        ExprType::LiteralInt8(*s as i32)
                    })),
                }),
                ScalarValue::Int16(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Int16, |s| {
                        ExprType::LiteralInt16(*s as i32)
                    })),
                }),
                ScalarValue::Int32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Int32, |s| {
                        ExprType::LiteralInt32(*s)
                    })),
                }),
                ScalarValue::Int64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Int64, |s| {
                        ExprType::LiteralInt64(*s)
                    })),
                }),
                ScalarValue::UInt8(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Uint8, |s| {
                        ExprType::LiteralUint8(*s as u32)
                    })),
                }),
                ScalarValue::UInt16(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Uint16, |s| {
                        ExprType::LiteralUint16(*s as u32)
                    })),
                }),
                ScalarValue::UInt32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Uint32, |s| {
                        ExprType::LiteralUint32(*s)
                    })),
                }),
                ScalarValue::UInt64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Uint64, |s| {
                        ExprType::LiteralUint64(*s)
                    })),
                }),
                ScalarValue::Float32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Float, |s| {
                        ExprType::LiteralF32(*s)
                    })),
                }),
                ScalarValue::Float64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, protobuf::ArrowType::Double, |s| {
                        ExprType::LiteralF64(*s)
                    })),
                }),
                other => Err(BallistaError::General(format!(
                    "to_proto unsupported scalar value {:?}",
                    other
                ))),
            },
            Expr::BinaryExpr { left, op, right } => {
                let binary_expr = Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::BinaryExpr(binary_expr)),
                })
            }
            Expr::AggregateFunction {
                ref fun, ref args, ..
            } => {
                let aggr_function = match fun {
                    AggregateFunction::Min => protobuf::AggregateFunction::Min,
                    AggregateFunction::Max => protobuf::AggregateFunction::Max,
                    AggregateFunction::Sum => protobuf::AggregateFunction::Sum,
                    AggregateFunction::Avg => protobuf::AggregateFunction::Avg,
                    AggregateFunction::Count => protobuf::AggregateFunction::Count,
                };

                let arg = &args[0];
                let aggregate_expr = Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
                })
            }
            Expr::ScalarVariable(_) => unimplemented!(),
            Expr::ScalarFunction { .. } => unimplemented!(),
            Expr::ScalarUDF { .. } => unimplemented!(),
            Expr::AggregateUDF { .. } => unimplemented!(),
            Expr::Not(expr) => {
                let expr = Box::new(protobuf::Not {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::NotExpr(expr)),
                })
            }
            Expr::IsNull(expr) => {
                let expr = Box::new(protobuf::IsNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNullExpr(expr)),
                })
            }
            Expr::IsNotNull(expr) => {
                let expr = Box::new(protobuf::IsNotNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNotNullExpr(expr)),
                })
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Box::new(protobuf::BetweenNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    negated: *negated,
                    low: Some(Box::new(low.as_ref().try_into()?)),
                    high: Some(Box::new(high.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Between(expr)),
                })
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        Ok(protobuf::WhenThen {
                            when_expr: Some(w.as_ref().try_into()?),
                            then_expr: Some(t.as_ref().try_into()?),
                        })
                    })
                    .collect::<Result<Vec<protobuf::WhenThen>, BallistaError>>()?;
                let expr = Box::new(protobuf::CaseNode {
                    expr: match expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                    when_then_expr,
                    else_expr: match else_expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Case(expr)),
                })
            }
            Expr::Cast { expr, data_type } => {
                let expr = Box::new(protobuf::CastNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    arrow_type: to_proto_arrow_type(data_type)?.into(),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Cast(expr)),
                })
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = Box::new(protobuf::SortExprNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Sort(expr)),
                })
            }
            Expr::Negative(expr) => {
                let expr = Box::new(protobuf::NegativeNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::Negative(expr)),
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Box::new(protobuf::InListNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    list: list
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    negated: *negated,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::InList(expr)),
                })
            }
            Expr::Wildcard => Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::Wildcard(true)),
            }),
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
        DataType::Boolean => Ok(protobuf::ArrowType::Bool),
        DataType::Int8 => Ok(protobuf::ArrowType::Int8),
        DataType::Int16 => Ok(protobuf::ArrowType::Int16),
        DataType::Int32 => Ok(protobuf::ArrowType::Int32),
        DataType::Int64 => Ok(protobuf::ArrowType::Int64),
        DataType::UInt8 => Ok(protobuf::ArrowType::Uint8),
        DataType::UInt16 => Ok(protobuf::ArrowType::Uint16),
        DataType::UInt32 => Ok(protobuf::ArrowType::Uint32),
        DataType::UInt64 => Ok(protobuf::ArrowType::Uint64),
        DataType::Float16 => Ok(protobuf::ArrowType::HalfFloat),
        DataType::Float32 => Ok(protobuf::ArrowType::Float),
        DataType::Float64 => Ok(protobuf::ArrowType::Double),
        DataType::Utf8 => Ok(protobuf::ArrowType::Utf8),
        DataType::Date32 => Ok(protobuf::ArrowType::Date32),
        DataType::Date64 => Ok(protobuf::ArrowType::Date64),
        DataType::Binary => Ok(protobuf::ArrowType::Binary),
        other => Err(BallistaError::General(format!(
            "logical_plan::to_proto() Unsupported data type {:?}",
            other
        ))),
    }
}
