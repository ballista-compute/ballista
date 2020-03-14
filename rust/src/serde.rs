use crate::error::BallistaError;
use crate::logical_plan::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use crate::protobuf;

use std::convert::TryInto;

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
                let input: protobuf::LogicalPlanNode = input.as_ref().clone().try_into()?;
                let mut node = empty_plan_node();
                node.input = Some(Box::new(input));
                Ok(node)
            }
            LogicalPlan::Selection { expr, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().clone().try_into()?;
                let selection = protobuf::SelectionNode {
                    expr: Some(expr.as_ref().clone().try_into()?)
                };
                let mut node = empty_plan_node();
                node.input = Some(Box::new(input));
                node.selection = Some(selection);
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
