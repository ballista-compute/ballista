use crate::error::{ballista_error, BallistaError};
use crate::plan::{Action, TableMeta};
use crate::protobuf;

use datafusion::logicalplan::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarValue};

use prost::Message;

use arrow::datatypes::{DataType, Field, Schema};

use std::convert::TryInto;
use std::io::Cursor;
use std::sync::Arc;

impl TryInto<LogicalPlan> for protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        if let Some(projection) = self.projection {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            LogicalPlanBuilder::from(&input)
                .project(
                    projection
                        .expr
                        .iter()
                        .map(|expr| expr.to_owned().try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(selection) = self.selection {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            let expr: protobuf::LogicalExprNode = selection.expr.expect("expression required");
            LogicalPlanBuilder::from(&input)
                .filter(expr.try_into()?)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(aggregate) = self.aggregate {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            let group_expr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_expr = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            LogicalPlanBuilder::from(&input)
                .aggregate(group_expr, aggr_expr)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(scan) = self.scan {
            let schema = Schema::new(
                scan.schema
                    .unwrap()
                    .columns
                    .iter()
                    .map(|field| Field::new(&field.name, DataType::Utf8, true))
                    .collect(),
            );

            LogicalPlanBuilder::scan("", &scan.table_name, &schema, None)?
                .build()
                .map_err(|e| e.into())
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical plan '{:?}'",
                self
            )))
        }
    }
}

impl TryInto<Expr> for protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        if let Some(binary_expr) = self.binary_expr {
            Ok(Expr::BinaryExpr {
                left: Arc::new(parse_required_expr(binary_expr.l)?),
                op: Operator::Eq, //TODO parse binary_expr.op.clone(),
                right: Arc::new(parse_required_expr(binary_expr.r)?),
            })
        } else if self.has_column_index {
            Ok(Expr::Column(self.column_index as usize))
        } else if self.has_column_name {
            Ok(Expr::UnresolvedColumn(self.column_name))
        } else if self.has_literal_string {
            Ok(Expr::Literal(ScalarValue::Utf8(
                self.literal_string.clone(),
            )))
        } else if let Some(aggregate_expr) = self.aggregate_expr {
            Ok(Expr::AggregateFunction {
                name: "MAX".to_string(), //TODO
                args: vec![parse_required_expr(aggregate_expr.expr)?],
                return_type: DataType::Boolean, //TODO
            })
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical expression '{:?}'",
                self
            )))
        }
    }
}

fn parse_required_expr(p: Option<Box<protobuf::LogicalExprNode>>) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().to_owned().try_into(),
        None => Err(ballista_error("Missing required expression")),
    }
}

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::RemoteQuery { plan, tables } => {
                let plan_proto: protobuf::LogicalPlanNode = plan.try_into()?;

                let table_meta: Vec<protobuf::TableMeta> = tables
                    .iter()
                    .map(|t| match t {
                        TableMeta::Csv {
                            table_name,
                            path,
                            has_header,
                            schema,
                        } => {
                            let schema: Result<protobuf::Schema, _> = schema.clone().try_into();

                            schema.and_then(|schema| {
                                let csv_meta = protobuf::CsvFileMeta {
                                    has_header: *has_header,
                                    schema: Some(schema),
                                };

                                Ok(protobuf::TableMeta {
                                    table_name: table_name.to_owned(),
                                    filename: path.to_owned(),
                                    csv_meta: Some(csv_meta),
                                })
                            })
                        }
                        _ => unimplemented!(),
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(protobuf::Action {
                    query: Some(plan_proto),
                    table_meta: table_meta,
                })
            }
        }
    }
}

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        if self.query.is_some() {
            let plan: LogicalPlan = self.query.unwrap().try_into()?;
            let tables = self
                .table_meta
                .iter()
                .map(|t| {
                    if t.csv_meta.is_some() {
                        //TODO fix the ugly code and make safe
                        let csv_meta = t.csv_meta.as_ref().unwrap();
                        let schema: Result<Schema, _> =
                            csv_meta.schema.as_ref().unwrap().clone().try_into();
                        schema.and_then(|schema| {
                            Ok(TableMeta::Csv {
                                table_name: t.table_name.to_owned(),
                                path: t.filename.to_owned(),
                                has_header: csv_meta.has_header,
                                schema: schema,
                            })
                        })
                    } else {
                        unimplemented!()
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Action::RemoteQuery { plan, tables })
        } else {
            Err(BallistaError::NotImplemented(format!("{:?}", self)))
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

fn from_proto_arrow_type(dt: i32 /*protobuf::ArrowType*/) -> Result<DataType, BallistaError> {
    //TODO how to match on protobuf enums ?
    match dt {
        /*protobuf::ArrowType::Uint8*/ 2 => Ok(DataType::UInt8),
        /*protobuf::ArrowType::Int8*/ 3 => Ok(DataType::Int8),
        /*protobuf::ArrowType::UInt16*/ 4 => Ok(DataType::UInt16),
        /*protobuf::ArrowType::Int16*/ 5 => Ok(DataType::Int16),
        /*protobuf::ArrowType::UInt32*/ 6 => Ok(DataType::UInt32),
        /*protobuf::ArrowType::Int32*/ 7 => Ok(DataType::Int32),
        /*protobuf::ArrowType::UInt64*/ 8 => Ok(DataType::UInt64),
        /*protobuf::ArrowType::Int64*/ 9 => Ok(DataType::Int64),
        /*protobuf::ArrowType::Float*/ 11 => Ok(DataType::Float32),
        /*protobuf::ArrowType::Double*/ 12 => Ok(DataType::Float64),
        /*protobuf::ArrowType::Utf8*/ 13 => Ok(DataType::Utf8),
        other => Err(BallistaError::General(format!(
            "Unsupported data type {:?}",
            other
        ))),
    }
}

impl TryInto<Schema> for protobuf::Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<Schema, Self::Error> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let dt: Result<DataType, _> = from_proto_arrow_type(c.arrow_type);
                dt.and_then(|dt| Ok(Field::new(&c.name, dt, c.nullable)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

impl TryInto<protobuf::LogicalPlanNode> for LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::TableScan {
                table_name,
                table_schema,
                ..
            } => {
                let mut node = empty_plan_node();

                let schema: protobuf::Schema = table_schema.as_ref().to_owned().try_into()?;

                node.scan = Some(protobuf::ScanNode {
                    table_name: table_name.clone(),
                    projection: vec![],
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
                    "MAX" => Ok(1), // TODO use protobuf enum
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

pub fn decode_protobuf(bytes: &Vec<u8>) -> Result<Action, BallistaError> {
    let mut buf = Cursor::new(bytes);
    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::plan::*;
    use crate::protobuf;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logicalplan::{col, lit_str, Expr, LogicalPlanBuilder};
    use std::convert::TryInto;

    #[test]
    fn roundtrip() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan("default", "employee", &schema, None)
            .and_then(|plan| plan.filter(col("state").eq(&lit_str("CO"))))
            .and_then(|plan| plan.project(vec![col("id")]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let action = Action::RemoteQuery {
            plan: plan.clone(),
            tables: vec![TableMeta::Csv {
                table_name: "employee".to_owned(),
                has_header: true,
                path: "/foo/bar.csv".to_owned(),
                schema: schema.clone(),
            }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    #[test]
    fn roundtrip_aggregate() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan("default", "employee", &schema, None)
            .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let action = Action::RemoteQuery {
            plan: plan.clone(),
            tables: vec![TableMeta::Csv {
                table_name: "employee".to_owned(),
                has_header: true,
                path: "/foo/bar.csv".to_owned(),
                schema: schema.clone(),
            }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    fn max(expr: Expr) -> Expr {
        Expr::AggregateFunction {
            name: "MAX".to_owned(),
            args: vec![expr],
            return_type: DataType::Float64,
        }
    }
}
