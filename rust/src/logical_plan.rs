use crate::error::{ballista_error, BallistaError, Result};

#[derive(Debug, Clone)]
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
    Or(Box<LogicalExpr>, Box<LogicalExpr>),
}

impl Into<String> for LogicalExpr {
    fn into(self) -> String {
        format_expr(&self)
    }
}

impl Into<String> for &LogicalExpr {
    fn into(self) -> String {
        format_expr(self)
    }
}

/// Produce a human readable representation of an expression
fn format_expr(expr: &LogicalExpr) -> String {
    match expr {
        LogicalExpr::Column(name) => format!("#{}", name),
        LogicalExpr::LiteralString(str) => str.to_owned(),
        LogicalExpr::Eq(l, r) => format!("{}=={}", format_expr(l), format_expr(r)),
        _ => format!("{:?}", expr),
    }
}

fn format_expr_list(expr: &Vec<LogicalExpr>) -> String {
    expr.iter()
        .map(|expr| expr.into())
        .collect::<Vec<String>>()
        .join(",")
}

/// Create a white space string to indent to the given indent level (two spaces per indent)
fn indent_str(i: usize) -> String {
    (0..i)
        .map(|_| "".to_owned())
        .collect::<Vec<String>>()
        .join("  ")
}

#[derive(Debug, Clone)]
pub enum LogicalAggregateExpr {
    Min(LogicalExpr),
    Max(LogicalExpr),
    Sum(LogicalExpr),
    Avg(LogicalExpr),
    Count(LogicalExpr),
    CountDistinct(LogicalExpr),
}

#[derive(Debug, Clone)]
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
    },
}

impl LogicalPlan {
    /// Produce a human readable representation of the logical plan
    pub fn pretty_print(&self) -> String {
        self.to_string(0)
    }

    fn to_string(&self, indent: usize) -> String {
        let tabs = indent_str(indent);
        match &self {
            Self::Scan { filename } => format!("{}Scan: {}", tabs, filename),
            Self::Projection { expr, input } => format!(
                "{}Projection: {}\n{}",
                tabs,
                format_expr_list(expr),
                input.to_string(indent + 1)
            ),
            Self::Selection { expr, input } => format!(
                "{}Selection: {}\n{}",
                tabs,
                format_expr(expr),
                input.to_string(indent + 1)
            ),
            Self::Aggregate {
                group_expr,
                aggr_expr,
                input,
            } => format!("{}Aggregate", tabs),
        }
    }
}

trait Relation {
    fn project(&self, expr: Vec<LogicalExpr>) -> Box<dyn Relation>;
    fn filter(&self, expr: LogicalExpr) -> Box<dyn Relation>;
}

pub struct LogicalPlanBuilder {
    plan: Option<LogicalPlan>,
}

impl LogicalPlanBuilder {

    pub fn new() -> Self {
        Self { plan: None }
    }

    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan: Some(plan) }
    }

    pub fn project(&self, expr: Vec<LogicalExpr>) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            Some(plan) => Ok(LogicalPlanBuilder::from(LogicalPlan::Projection {
                expr,
                input: Box::new(plan.clone()),
            })),
            _ => Err(ballista_error("Cannot apply a projection to an empty plan")),
        }
    }

    pub fn filter(&self, expr: LogicalExpr) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            Some(plan) => Ok(LogicalPlanBuilder::from(LogicalPlan::Selection {
                expr: Box::new(expr),
                input: Box::new(plan.clone()),
            })),
            _ => Err(ballista_error("Cannot apply a selection to an empty plan")),
        }
    }

    pub fn scan(&self, filename: &str) -> Result<LogicalPlanBuilder> {
        match &self.plan {
            None => Ok(LogicalPlanBuilder::from(LogicalPlan::Scan {
                filename: filename.to_owned(),
            })),
            _ => Err(ballista_error("Cannot apply a scan to a non-empty plan")),
        }
    }

    pub fn build(&self) -> Result<LogicalPlan> {
        match &self.plan {
            Some(plan) => Ok(plan.clone()),
            _ => Err(ballista_error("Cannot build an empty plan")),
        }
    }
}

pub fn col(name: &str) -> LogicalExpr {
    LogicalExpr::Column(name.to_owned())
}

pub fn lit_str(str: &str) -> LogicalExpr {
    LogicalExpr::LiteralString(str.to_owned())
}

//TODO use macros to implement the other binary expressions
pub fn eq(l: LogicalExpr, r: LogicalExpr) -> LogicalExpr {
    LogicalExpr::Eq(Box::new(l), Box::new(r))
}

#[cfg(test)]
mod tests {
    use super::LogicalPlan::*;
    use super::*;
    use crate::logical_plan::LogicalPlanBuilder;

    #[test]
    fn plan_builder() -> Result<()> {
        let plan = LogicalPlanBuilder::new()
            .scan("employee.csv")?
            .filter(eq(col("state"), lit_str("CO")))?
            .project(vec![col("state")])?
            .build()?;

        let expected_str = "Projection: #state\nSelection: #state==CO\n  Scan: employee.csv";

        let plan_str = plan.pretty_print();

        assert_eq!(expected_str, plan_str);

        Ok(())
    }
}
