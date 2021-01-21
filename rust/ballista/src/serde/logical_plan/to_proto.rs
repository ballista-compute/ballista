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

use std::{convert::TryInto, unimplemented};

use crate::serde::{empty_logical_plan_node, protobuf, BallistaError};

use arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::{datasource::{parquet::ParquetTable, CsvFile},
                 logical_plan::{Expr, JoinType, LogicalPlan},
                 physical_plan::aggregates::AggregateFunction,
                 scalar::ScalarValue};
use protobuf::logical_expr_node::ExprType;

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

                let columns = projected_schema.fields().iter().map(|f| f.name().to_owned()).collect();

                let projection = Some(protobuf::ProjectionColumns { columns });

                let schema: protobuf::Schema = schema.as_ref().into();

                let filters: Vec<protobuf::LogicalExprNode> = filters.iter().map(|filter| filter.try_into()).collect::<Result<Vec<_>, _>>()?;

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

                    Err(BallistaError::General(format!("logical plan to_proto unsupported table provider {:?}", source)))
                }
            },
            LogicalPlan::Projection { expr, input, .. } => {

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                node.projection = Some(protobuf::ProjectionNode {
                    expr: expr.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?,
                });

                Ok(node)
            },
            LogicalPlan::Filter { predicate, input } => {

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                node.selection = Some(protobuf::SelectionNode {
                    expr: Some(predicate.try_into()?),
                });

                Ok(node)
            },
            LogicalPlan::Aggregate {
                input, group_expr, aggr_expr, ..
            } => {

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                node.aggregate = Some(protobuf::AggregateNode {
                    group_expr: group_expr.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?,
                    aggr_expr:  aggr_expr.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?,
                });

                Ok(node)
            },
            LogicalPlan::Join {
                left, right, on, join_type, ..
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
            },
            LogicalPlan::Limit { input, n } => {

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                node.limit = Some(protobuf::LimitNode { limit: *n as u32 });

                Ok(node)
            },
            LogicalPlan::Sort { input, expr } => {

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                let selection_expr: Vec<protobuf::LogicalExprNode> = expr.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?;

                node.sort = Some(protobuf::SortNode { expr: selection_expr });

                Ok(node)
            },
            LogicalPlan::Repartition { input, partitioning_scheme } => {

                use datafusion::logical_plan::Partitioning;

                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;

                let mut node = empty_logical_plan_node();

                node.input = Some(Box::new(input));

                //Assumed common usize field was batch size
                //Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, batch_size) => PartitionMethod::Hash(protobuf::HashRepartition {
                        hash_expr:  exprs.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?,
                        batch_size: *batch_size as u64,
                    }),
                    Partitioning::RoundRobinBatch(batch_size) => PartitionMethod::RoundRobin(*batch_size as u64),
                };

                node.repartition = Some(protobuf::RepartitionNode {
                    partition_method: Some(pb_partition_method),
                });

                Ok(node)
            },
            LogicalPlan::EmptyRelation { produce_one_row, .. } => {

                let mut node = empty_logical_plan_node();

                node.empty_relation = Some(protobuf::EmptyRelationNode {
                    produce_one_row: *produce_one_row,
                });

                Ok(node)
            },
            LogicalPlan::CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                schema: df_schema,
            } => {

                let mut node = empty_logical_plan_node();

                use datafusion::sql::parser::FileType;

                let schema: Schema = df_schema.as_ref().clone().into();

                let pb_schema: protobuf::Schema = (&schema)
                    .try_into()
                    .map_err(|e| BallistaError::General(format!("Could not convert schema into protobuf: {:?}", e)))?;

                let pb_file_type: protobuf::FileType = match file_type {
                    FileType::NdJson => protobuf::FileType::NdJson,
                    FileType::Parquet => protobuf::FileType::Parquet,
                    FileType::CSV => protobuf::FileType::Csv,
                };

                node.create_external_table = Some(protobuf::CreateExternalTableNode {
                    name:       name.clone(),
                    location:   location.clone(),
                    file_type:  pb_file_type as i32,
                    has_header: *has_header,
                    schema:     Some(pb_schema),
                });

                Ok(node)
            },
            LogicalPlan::Explain { verbose, plan, .. } => {

                let mut node = empty_logical_plan_node();

                let input: protobuf::LogicalPlanNode = plan.as_ref().try_into()?;

                node.input = Some(Box::new(input));

                node.explain = Some(protobuf::ExplainNode { verbose: *verbose });

                Ok(node)
            },
            LogicalPlan::Extension { .. } => unimplemented!(),
            /* _ => Err(BallistaError::General(format!(
             *     "logical plan to_proto {:?}",
             *     self
             * ))), */
        }
    }
}

fn create_proto_scalar<I, T: FnOnce(&I) -> ExprType>(v: &Option<I>, null_arrow_type: protobuf::ScalarType, constructor: T) -> ExprType {

    v.as_ref().map(constructor).unwrap_or(ExprType::LiteralNull(null_arrow_type as i32))
}

impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        use protobuf::ScalarType;
        match self {
            Expr::Column(name) => {

                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::ColumnName(name.clone())),
                };

                Ok(expr)
            },
            Expr::Alias(expr, alias) => {

                let alias = Box::new(protobuf::AliasNode {
                    expr:  Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                });

                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Alias(alias)),
                };

                Ok(expr)
            },
            Expr::Literal(value) => match value {
                ScalarValue::Utf8(s) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(s, ScalarType::Utf8, |s| ExprType::LiteralString(s.to_owned()))),
                }),
                ScalarValue::Int8(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Int8, |s| ExprType::LiteralInt8(*s as i32))),
                }),
                ScalarValue::Int16(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Int16, |s| ExprType::LiteralInt16(*s as i32))),
                }),
                ScalarValue::Int32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Int32, |s| ExprType::LiteralInt32(*s))),
                }),
                ScalarValue::Int64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Int64, |s| ExprType::LiteralInt64(*s))),
                }),
                ScalarValue::UInt8(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Uint8, |s| ExprType::LiteralUint8(*s as u32))),
                }),
                ScalarValue::UInt16(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Uint16, |s| ExprType::LiteralUint16(*s as u32))),
                }),
                ScalarValue::UInt32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Uint32, |s| ExprType::LiteralUint32(*s))),
                }),
                ScalarValue::UInt64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Uint64, |s| ExprType::LiteralUint64(*s))),
                }),
                ScalarValue::Float32(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Float32, |s| ExprType::LiteralF32(*s))),
                }),
                ScalarValue::Float64(n) => Ok(protobuf::LogicalExprNode {
                    expr_type: Some(create_proto_scalar(n, ScalarType::Float64, |s| ExprType::LiteralF64(*s))),
                }),
                ScalarValue::Date32(n)=> Ok(protobuf::LogicalExprNode{
                    expr_type: Some(create_proto_scalar(n, ScalarType::Date32, |s| ExprType::LiteralDate32(*s))),
                }),

                ScalarValue::Boolean(n) => Ok(protobuf::LogicalExprNode{
                    expr_type: Some(create_proto_scalar(n, ScalarType::Bool, |s| ExprType::LiteralBool(*s))),
                }),
                ScalarValue::LargeUtf8(n) => Ok(protobuf::LogicalExprNode{
                    expr_type: Some(create_proto_scalar(n, ScalarType::LargeUtf8, |s| ExprType::LiteralString(*s))),
                }),
                ScalarValue::List(list, datatype) => Err(BallistaError::General(String::from("Scalar value list is unimplemented"))),
                ScalarValue::TimeMicrosecond(n) => Ok(protobuf::LogicalExprNode{
                    expr_type: Some(create_proto_scalar(n, ScalarType::TimeMicrosecond, |s| ExprType::LiteralTimeMicrosecond(*s))),
                }),
                ScalarValue::TimeNanosecond(n) => Ok(protobuf::LogicalExprNode{
                    expr_type: Some(create_proto_scalar(n, ScalarType::TimeNanosecond, |s| ExprType::LiteralTimeNanosecond(*s))),
                }),
            },
            Expr::BinaryExpr { left, op, right } => {

                let binary_expr = Box::new(protobuf::BinaryExprNode {
                    l:  Some(Box::new(left.as_ref().try_into()?)),
                    r:  Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::BinaryExpr(binary_expr)),
                })
            },
            Expr::AggregateFunction { ref fun, ref args, .. } => {

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
                    expr:          Some(Box::new(arg.try_into()?)),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
                })
            },
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
            },
            Expr::IsNull(expr) => {

                let expr = Box::new(protobuf::IsNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNullExpr(expr)),
                })
            },
            Expr::IsNotNull(expr) => {

                let expr = Box::new(protobuf::IsNotNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNotNullExpr(expr)),
                })
            },
            Expr::Between { expr, negated, low, high } => {

                let expr = Box::new(protobuf::BetweenNode {
                    expr:    Some(Box::new(expr.as_ref().try_into()?)),
                    negated: *negated,
                    low:     Some(Box::new(low.as_ref().try_into()?)),
                    high:    Some(Box::new(high.as_ref().try_into()?)),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Between(expr)),
                })
            },
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
            },
            Expr::Cast { expr, data_type } => {

                let expr = Box::new(protobuf::CastNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    arrow_type: data_type.try_into()?,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Cast(expr)),
                })
            },
            Expr::Sort { expr, asc, nulls_first } => {

                let expr = Box::new(protobuf::SortExprNode {
                    expr:        Some(Box::new(expr.as_ref().try_into()?)),
                    asc:         *asc,
                    nulls_first: *nulls_first,
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Sort(expr)),
                })
            },
            Expr::Negative(expr) => {

                let expr = Box::new(protobuf::NegativeNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::Negative(expr)),
                })
            },
            Expr::InList { expr, list, negated } => {

                let expr = Box::new(protobuf::InListNode {
                    expr:    Some(Box::new(expr.as_ref().try_into()?)),
                    list:    list.iter().map(|expr| expr.try_into()).collect::<Result<Vec<_>, BallistaError>>()?,
                    negated: *negated,
                });

                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::InList(expr)),
                })
            },
            Expr::Wildcard => Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::Wildcard(true)),
            }),
            /* _ => Err(BallistaError::General(format!(
             *     "logical expr to_proto {:?}",
             *     self
             * ))), */
        }
    }
}


impl protobuf::TimeUnit{
    fn from_arrow(tu: &arrow::datatypes::TimeUnit)->Self{
        tu.into()
    }
}

impl protobuf::DateUnit{
    fn from_arrow(du: &arrow::datatypes::DateUnit)->Self{
        du.into()
    }
}

impl protobuf::IntervalUnit{
    fn from_arrow(iu: &arrow::datatypes::IntervalUnit)->Self{
        iu.into()
    }
}

impl Into<protobuf::TimeUnit> for &arrow::datatypes::TimeUnit {
    fn into(self) -> protobuf::TimeUnit {
        match self {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::TimeMillisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
}

impl Into<protobuf::DateUnit> for &arrow::datatypes::DateUnit {
    fn into(self) -> protobuf::DateUnit {

        match self {
            arrow::datatypes::DateUnit::Day => protobuf::DateUnit::Day,
            arrow::datatypes::DateUnit::Millisecond => protobuf::DateUnit::DateMillisecond,
        }
    }
}

impl Into<protobuf::IntervalUnit> for &arrow::datatypes::IntervalUnit {
    fn into(self) -> protobuf::IntervalUnit {

        match self {
            arrow::datatypes::IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            arrow::datatypes::IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
        }
    }
}

impl Into<protobuf::Field> for &arrow::datatypes::Field {
    fn into(self) -> protobuf::Field {

        protobuf::Field {
            name:       self.name().clone(),
            arrow_type: Some(Box::new(self.data_type().into())),
            nullable:   self.is_nullable(),
            children:   Vec::new(),
        }
    }
}



use std::default::Default;
impl Into<protobuf::ArrowType> for &DataType {

    fn into(self) -> protobuf::ArrowType{

        use protobuf::{arrow_type::Type, EmptyMessage};

        // All variants containing (..) have not additional meta data to serialize and are filled with a default zero sized
        // struct EmptyMessage
        let res: Type = match self {
            DataType::Null => Type::None(EmptyMessage {}),
            DataType::Boolean => Type::Bool(EmptyMessage {}),
            DataType::Int8 => Type::Int8(EmptyMessage {}),
            DataType::Int16 => Type::Int16(EmptyMessage {}),
            DataType::Int32 => Type::Int32(EmptyMessage {}),
            DataType::Int64 => Type::Int64(EmptyMessage {}),
            DataType::UInt8 => Type::Uint8(EmptyMessage {}),
            DataType::UInt16 => Type::Uint16(EmptyMessage {}),
            DataType::UInt32 => Type::Uint32(EmptyMessage {}),
            DataType::UInt64 => Type::Uint64(EmptyMessage {}),
            DataType::Float16 => Type::Float16(EmptyMessage {}),
            DataType::Float32 => Type::Float32(EmptyMessage {}),
            DataType::Float64 => Type::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => Type::Timestamp(protobuf::Timestamp {
                time_unit: <&arrow::datatypes::TimeUnit as Into<protobuf::TimeUnit>>::into(time_unit) as i32,
                timezone:  timezone.clone().unwrap_or_else(String::new),
            }),
            DataType::Date32(date_unit) => Type::Date32(protobuf::DateUnit::from_arrow(date_unit) as i32),
            DataType::Date64(date_unit) => Type::Date64(protobuf::DateUnit::from_arrow(date_unit) as i32),
            DataType::Time32(time_unit) => Type::Time32(protobuf::TimeUnit::from_arrow(time_unit) as i32),
            DataType::Time64(time_unit) => Type::Time64(protobuf::TimeUnit::from_arrow(time_unit) as i32),
            DataType::Duration(duration_unit) => Type::Duration(protobuf::TimeUnit::from_arrow(duration_unit) as i32),
            DataType::Interval(interval_unit) => Type::Interval(protobuf::IntervalUnit::from_arrow(interval_unit) as i32),
            DataType::Binary => Type::Binary(EmptyMessage {}),
            DataType::FixedSizeBinary(length) => Type::FixedSizeBinary(*length),
            DataType::LargeBinary => Type::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Type::Utf8(EmptyMessage {}),
            DataType::LargeUtf8 => Type::LargeUtf8(EmptyMessage {}),
            DataType::List(field) => Type::List(Box::new(protobuf::List {
                field_type: Some(Box::new(field.as_ref().into())),
            })),
            DataType::FixedSizeList(field, length) => Type::FixedSizeList(Box::new(protobuf::FixedSizeList {
                field_type: Some(Box::new(field.as_ref().into())),
                list_size:  *length,
            })), 
            DataType::LargeList(field) => Type::LargeList(Box::new(protobuf::List {
                field_type: Some(Box::new(field.as_ref().into())),
            })),
            DataType::Struct(sub_field_types) => Type::Struct(protobuf::Struct {
                sub_field_types: sub_field_types.iter().map(|field| field.into()).collect::<Vec<protobuf::Field>>(),
            }),
            DataType::Union(union_fields) => Type::Union(protobuf::Union {
                union_types: union_fields.iter().map(|field| field.into()).collect::<Vec<protobuf::Field>>(),
            }),
            DataType::Dictionary(key_type, value_type) => Type::Dictionary(Box::new(protobuf::Dictionary {
                key:   Some(Box::new(key_type.as_ref().into())),
                value: Some(Box::new(value_type.as_ref().into())),
            })),
            DataType::Decimal(whole, fractional) => Type::Decimal(protobuf::Decimal {
                whole:      *whole as u64,
                fractional: *fractional as u64,
            }),
        };
        protobuf::NewArrowType { r#type: Some(res) }
    }
}



impl Into<protobuf::Schema> for &Schema {
    fn into(self) -> protobuf::Schema {
        use arrow::datatypes::Field;
        protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(|field: &Field| {

                    protobuf::Field{
                        name: field.name().to_owned(),
                        arrow_type: Some(Box::new(field.data_type().into())),
                        nullable: field.is_nullable(),
                        children: Vec::new(),
                    }
                })
                .collect::<Vec<_>>(),
        }
    }
}
