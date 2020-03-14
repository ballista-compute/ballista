use crate::error::{ballista_error, BallistaError};
use crate::logical_plan::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use crate::protobuf;

use std::convert::TryInto;

impl TryInto<LogicalPlan> for protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        if let Some(projection) = self.projection {
            Ok(LogicalPlan::Projection {
                expr: vec![],
                input: Box::new(self.input.unwrap().as_ref().to_owned().try_into()?),
            })
        } else if let Some(selection) = self.selection {
            let expr: protobuf::LogicalExprNode = selection.expr.expect("expression required");
            Ok(LogicalPlan::Selection {
                expr: Box::new(expr.try_into()?),
                input: Box::new(self.input.unwrap().as_ref().to_owned().try_into()?),
            })
        } else if let Some(scan) = self.file {
            Ok(LogicalPlan::Scan {
                filename: scan.filename.clone(),
            })
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical plan '{:?}'",
                self
            )))
        }
    }
}

impl TryInto<LogicalExpr> for protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalExpr, Self::Error> {
        if let Some(binary_expr) = self.binary_expr {
            match binary_expr.op.as_ref() {
                //TODO use macros to implement all binary expressions
                "eq" => Ok(LogicalExpr::Eq(
                    Box::new(parse_required_expr(binary_expr.l)?),
                    Box::new(parse_required_expr(binary_expr.r)?),
                )),
                other => Err(ballista_error(&format!(
                    "Unsupported binary operator '{}'",
                    other
                ))),
            }
        } else if self.has_column_name {
            Ok(LogicalExpr::Column(self.column_name.clone()))
        } else if self.has_literal_string {
            Ok(LogicalExpr::LiteralString(self.literal_string.clone()))
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical expression '{:?}'",
                self
            )))
        }
    }
}

fn parse_required_expr(
    p: Option<Box<protobuf::LogicalExprNode>>,
) -> Result<LogicalExpr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().to_owned().try_into(),
        None => Err(ballista_error("Missing required expression")),
    }
}

impl TryInto<protobuf::LogicalPlanNode> for LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::Scan { filename } => {
                let mut node = empty_plan_node();
                node.file = Some(protobuf::FileNode {
                    filename: filename.clone(),
                    schema: None,
                    projection: vec![],
                });
                Ok(node)
            }
            LogicalPlan::Projection { expr, input } => {
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
                    expr: Some(expr.as_ref().to_owned().try_into()?),
                });
                Ok(node)
            }
            _ => Err(BallistaError::NotImplemented(format!("{:?}", self))),
        }
    }
}

impl TryInto<protobuf::LogicalExprNode> for LogicalExpr {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        match self {
            LogicalExpr::Column(name) => {
                let mut expr = empty_expr_node();
                expr.has_column_name = true;
                expr.column_name = name.clone();
                Ok(expr)
            }
            LogicalExpr::LiteralString(str) => {
                let mut expr = empty_expr_node();
                expr.has_literal_string = true;
                expr.literal_string = str.clone();
                Ok(expr)
            }
            LogicalExpr::Eq(l, r) => {
                let mut expr = empty_expr_node();
                expr.binary_expr = Some(Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(l.as_ref().to_owned().try_into()?)),
                    r: Some(Box::new(r.as_ref().to_owned().try_into()?)),
                    op: "eq".to_owned(),
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

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::logical_plan::*;
    use crate::protobuf;
    use std::convert::TryInto;

    #[test]
    fn roundtrip() -> Result<()> {
        let plan = LogicalPlanBuilder::new()
            .scan("employee.csv")?
            .filter(eq(col("state"), lit_str("CO")))?
            .project(vec![col("state")])?
            .build()?;

        let proto: protobuf::LogicalPlanNode = plan.try_into()?;

        Ok(())
    }
}
