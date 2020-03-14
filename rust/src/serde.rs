use crate::error::{BallistaError, Result};
use crate::logical_plan::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use crate::protobuf;

fn from_plan(plan: &LogicalPlan) -> Result<protobuf::LogicalPlanNode> {
    match plan {
        LogicalPlan::Scan { filename } => {
            let node = empty_plan_node();
            Ok(node)
        }
        LogicalPlan::Projection { expr, input } => {
            let input = from_plan(&input)?;
            let node = empty_plan_node();
            Ok(node)
        }
        LogicalPlan::Selection { expr, input } => {
            let input = from_plan(&input)?;
            let node = empty_plan_node();
            Ok(node)
        }
        _ => Err(BallistaError::NotImplemented(format!("{:?}", plan))),
    }
}

fn from_expr(expr: &LogicalExpr) -> Result<protobuf::LogicalExprNode> {
    match expr {
        LogicalExpr::Column(name) => {
            let mut expr = empty_expr_node();
            expr.has_column_name = true;
            expr.column_name = name.clone();
            Ok(expr)
        }
        _ => Err(BallistaError::NotImplemented(format!("{:?}", expr))),
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
    use super::LogicalExpr::*;
    use super::LogicalPlan::*;
    use super::*;

    #[test]
    fn roundtrip() -> Result<()> {
        let scan = Scan {
            filename: "employee.csv".to_owned(),
        };

        let filter = Selection {
            input: Box::new(scan),
            expr: Box::new(Eq(
                Box::new(Column("state".to_owned())),
                Box::new(LiteralString("CO".to_owned())),
            )),
        };

        let projection = Projection {
            input: Box::new(filter),
            expr: vec![Column("state".to_owned())],
        };

        let proto = from_plan(&projection)?;

        Ok(())
    }
}
