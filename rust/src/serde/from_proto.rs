use crate::error::{ballista_error, BallistaError};
use crate::plan::{Action, TableMeta};
use crate::protobuf;

use datafusion::logicalplan::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarValue};

use arrow::datatypes::{DataType, Field, Schema};

use std::convert::TryInto;
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

fn parse_required_expr(p: Option<Box<protobuf::LogicalExprNode>>) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().to_owned().try_into(),
        None => Err(ballista_error("Missing required expression")),
    }
}
