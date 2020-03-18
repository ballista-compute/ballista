use crate::error::{ballista_error, BallistaError};
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
                    &projection
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
        } else if let Some(scan) = self.file {
            let schema = Schema::new(
                scan.schema
                    .unwrap()
                    .columns
                    .iter()
                    .map(|field| Field::new(&field.name, DataType::Utf8, true))
                    .collect(),
            );

            LogicalPlanBuilder::scan("", &scan.filename, &schema, None)?
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
        // } else if self.has_column_name {
        //     Ok(Expr::Column(self.column_name.clone()))
        } else if self.has_literal_string {
            Ok(Expr::Literal(ScalarValue::Utf8(
                self.literal_string.clone(),
            )))
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

                let schema = protobuf::Schema {
                    columns: table_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            protobuf::Field {
                                name: field.name().to_owned(),
                                arrow_type: 12, //TODO
                                nullable: true,
                                children: vec![],
                            }
                        })
                        .collect(),
                };

                node.file = Some(protobuf::FileNode {
                    filename: table_name.clone(),
                    schema: Some(schema),
                    projection: vec![],
                });
                Ok(node)
            }
            LogicalPlan::Projection {
                expr,
                input,
                ..
            } => {
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
            // Expr::ColumnName(name) => {
            //     let mut expr = empty_expr_node();
            //     expr.has_column_name = true;
            //     expr.column_name = name.clone();
            //     Ok(expr)
            // }
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
        file: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
    }
}

pub fn decode_protobuf(bytes: &Vec<u8>) -> Result<LogicalPlan, BallistaError> {
    let mut buf = Cursor::new(bytes);
    protobuf::LogicalPlanNode::decode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::protobuf;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logicalplan::{col, lit_str, LogicalPlan, LogicalPlanBuilder};
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
            .and_then(|plan| plan.filter(col(4).eq(&lit_str("CO"))))
            .and_then(|plan| plan.project(&vec![col(0)]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let proto: protobuf::LogicalPlanNode = plan.clone().try_into()?;

        let plan2: LogicalPlan = proto.try_into()?;

        assert_eq!(format!("{:?}", plan), format!("{:?}", plan2));

        Ok(())
    }
}
