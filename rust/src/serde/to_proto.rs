use crate::error::BallistaError;
use crate::plan::Action;
use crate::protobuf;

use crate::logicalplan::{Expr, LogicalPlan, ScalarValue};

use arrow::datatypes::{DataType, Schema};

use std::convert::TryInto;

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::Collect { plan } => {
                let plan_proto: protobuf::LogicalPlanNode = plan.try_into()?;

                // let table_meta: Vec<protobuf::TableMeta> = tables
                //     .iter()
                //     .map(|t| match t {
                //         TableMeta::Csv {
                //             table_name,
                //             path,
                //             has_header,
                //             schema,
                //         } => {
                //             let schema: Result<protobuf::Schema, _> = schema.clone().try_into();
                //
                //             schema.and_then(|schema| {
                //                 let csv_meta = protobuf::CsvFileMeta {
                //                     has_header: *has_header,
                //                     schema: Some(schema),
                //                 };
                //
                //                 Ok(protobuf::TableMeta {
                //                     table_name: table_name.to_owned(),
                //                     filename: path.to_owned(),
                //                     csv_meta: Some(csv_meta),
                //                 })
                //             })
                //         }
                //         _ => unimplemented!(),
                //     })
                //     .collect::<Result<Vec<_>, _>>()?;

                Ok(protobuf::Action {
                    query: Some(plan_proto),
                })
            }
        }
    }
}

impl TryInto<protobuf::Schema> for Schema {
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

impl TryInto<protobuf::LogicalPlanNode> for LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::FileScan {
                path,
                schema,
                projection,
                ..
            } => {
                let mut node = empty_plan_node();

                let projected_field_names = match projection {
                    Some(p) => p.iter().map(|i| schema.field(*i).name().clone()).collect(),
                    _ => vec![],
                };

                let schema: protobuf::Schema = schema.to_owned().try_into()?;

                node.scan = Some(protobuf::ScanNode {
                    path: path.clone(),
                    projection: projected_field_names,
                    schema: Some(schema),
                });
                Ok(node)
            }
            LogicalPlan::Projection { expr, input, .. } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().to_owned().try_into()?;
                let mut node = empty_plan_node();
                node.input = Some(Box::new(input));
                node.projection = Some(protobuf::ProjectionNode {
                    expr: expr
                        .iter()
                        .map(|expr| expr.to_owned().try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            LogicalPlan::Selection { expr, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().to_owned().try_into()?;
                let mut node = empty_plan_node();
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
                let input: protobuf::LogicalPlanNode = input.as_ref().to_owned().try_into()?;
                let mut node = empty_plan_node();
                node.input = Some(Box::new(input));
                node.aggregate = Some(protobuf::AggregateNode {
                    group_expr: group_expr
                        .iter()
                        .map(|expr| expr.to_owned().try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    aggr_expr: aggr_expr
                        .iter()
                        .map(|expr| expr.to_owned().try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            _ => Err(BallistaError::NotImplemented(format!("{:?}", self))),
        }
    }
}

impl TryInto<protobuf::LogicalExprNode> for Expr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            Expr::Column(index) => {
                let mut expr = empty_expr_node();
                expr.has_column_index = true;
                expr.column_index = index as u32;
                Ok(expr)
            }
            Expr::UnresolvedColumn(name) => {
                let mut expr = empty_expr_node();
                expr.has_column_name = true;
                expr.column_name = name.clone();
                Ok(expr)
            }
            Expr::Literal(ScalarValue::Utf8(str)) => {
                let mut expr = empty_expr_node();
                expr.has_literal_string = true;
                expr.literal_string = str.clone();
                Ok(expr)
            }
            Expr::BinaryExpr { left, op, right } => {
                let mut expr = empty_expr_node();
                expr.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().to_owned().try_into()?)),
                    r: Some(Box::new(right.as_ref().to_owned().try_into()?)),
                    op: format!("{:?}", op),
                }));
                Ok(expr)
            }
            Expr::AggregateFunction { name, args, .. } => {
                let mut expr = empty_expr_node();

                let aggr_function = match name.as_str() {
                    "MIN" => Ok(0),   // TODO use protobuf enum
                    "MAX" => Ok(1),   // TODO use protobuf enum
                    "SUM" => Ok(2),   // TODO use protobuf enum
                    "AVG" => Ok(3),   // TODO use protobuf enum
                    "COUNT" => Ok(4), // TODO use protobuf enum
                    other => Err(BallistaError::NotImplemented(format!(
                        "Aggregate function {:?}",
                        other
                    ))),
                }?;

                expr.aggregate_expr = Some(Box::new(protobuf::AggregateExprNode {
                    aggr_function,
                    expr: Some(Box::new(args[0].clone().try_into()?)),
                }));
                Ok(expr)
            }
            _ => Err(BallistaError::NotImplemented(format!("{:?}", self))),
        }
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
fn empty_plan_node() -> protobuf::LogicalPlanNode {
    protobuf::LogicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
    }
}
