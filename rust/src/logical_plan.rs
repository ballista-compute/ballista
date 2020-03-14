use crate::error::{BallistaError, Result, ballista_error};

#[derive(Debug,Clone)]
pub enum LogicalExpr {
    Column(String),
    ColumnIndex(usize),
    LiteralString(String),
    LiteralLong(i64),
    Eq(Box<LogicalExpr>, Box<LogicalExpr>),
    Lt(Box<LogicalExpr>, Box<LogicalExpr>),
    LtEq(Box<LogicalExpr>, Box<LogicalExpr>),
    Gt(Box<LogicalExpr>, Box<LogicalExpr>),
    GtEq(Box<LogicalExpr>, Box<LogicalExpr>),
    Add(Box<LogicalExpr>, Box<LogicalExpr>),
    Subtract(Box<LogicalExpr>, Box<LogicalExpr>),
    Multiply(Box<LogicalExpr>, Box<LogicalExpr>),
    Divide(Box<LogicalExpr>, Box<LogicalExpr>),
    Modulus(Box<LogicalExpr>, Box<LogicalExpr>),
    And(Box<LogicalExpr>, Box<LogicalExpr>),
    Or(Box<LogicalExpr>, Box<LogicalExpr>)
}

#[derive(Debug,Clone)]
pub enum LogicalAggregateExpr {
    Min(LogicalExpr),
    Max(LogicalExpr),
    Sum(LogicalExpr),
    Avg(LogicalExpr),
    Count(LogicalExpr),
    CountDistinct(LogicalExpr),
}

#[derive(Debug,Clone)]
pub enum LogicalPlan {
    Aggregate {
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<LogicalAggregateExpr>,
        input: Box<LogicalPlan>,
    },
    Projection {
        expr: Vec<LogicalExpr>,
        input: Box<LogicalPlan>,
    },
    Selection {
        expr: Box<LogicalExpr>,
        input: Box<LogicalPlan>,
    },
    Scan {
        filename: String,
    }
}

trait Relation {
    fn project(&self, expr: Vec<LogicalExpr>) -> Box<dyn Relation>;
    fn filter(&self, expr: LogicalExpr) -> Box<dyn Relation>;
}

struct LogicalPlanBuilder {
    plan: Option<LogicalPlan>
}

impl LogicalPlanBuilder {

    fn new() -> Self {
        Self { plan: None }
    }

    fn from(plan: LogicalPlan) -> Self {
        Self { plan: Some(plan) }
    }

    fn project(&self, expr: Vec<LogicalExpr>) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            Some(plan) => Ok(LogicalPlanBuilder::from(LogicalPlan::Projection {
                expr,
                input: Box::new(plan.clone())
            })),
            _ => Err(ballista_error("Cannot apply a projection to an empty plan"))
        }
    }

    fn filter(&self, expr: LogicalExpr) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            Some(plan) => Ok(LogicalPlanBuilder::from(LogicalPlan::Selection {
                expr: Box::new(expr),
                input: Box::new(plan.clone())
            })),
            _ => Err(ballista_error("Cannot apply a selection to an empty plan"))
        }
    }

    fn scan(&self, filename: &str) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            None => Ok(LogicalPlanBuilder::from(LogicalPlan::Scan {
                filename: filename.to_owned()
            })),
            _ => Err(ballista_error("Cannot apply a scan to a non-empty plan"))
        }
    }

    fn build(&self) -> Result<LogicalPlan> {
        match &self.plan {
            Some(plan) => Ok(plan.clone()),
            _ => Err(ballista_error("Cannot build an empty plan"))
        }
    }
}

fn col(name: &str) -> LogicalExpr {
    LogicalExpr::Column(name.to_owned())
}
fn lit_str(str: &str) -> LogicalExpr {
    LogicalExpr::LiteralString(str.to_owned())
}

fn eq(l: LogicalExpr, r: LogicalExpr) -> LogicalExpr {
    LogicalExpr::Eq(Box::new(l), Box::new(r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::LogicalPlan::*;
    use crate::logical_plan::LogicalPlanBuilder;

    #[test]
    fn plan_builder() -> Result<()> {

        let plan = LogicalPlanBuilder::new()
            .scan("employee.csv")?
            .filter(eq(col("state"), lit_str("CO")))?
            .project(vec![col("state")])?
            .build()?;

        let plan_str = format!("{:?}", plan);
        let expected_str =
            "Projection { \
                expr: [Column(\"state\")], \
                input: Selection { \
                    expr: Eq(Column(\"state\"), LiteralString(\"CO\")), \
                    input: Scan { \
                        filename: \"employee.csv\" \
                    } \
                } \
            }";

        assert_eq!(expected_str, plan_str);

        Ok(())
    }

}