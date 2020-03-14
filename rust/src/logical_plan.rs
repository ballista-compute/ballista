
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
    plan: LogicalPlan
}

impl LogicalPlanBuilder {

    fn from(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    fn project(&self, expr: Vec<LogicalExpr>) -> LogicalPlanBuilder {
        LogicalPlanBuilder::from(LogicalPlan::Projection {
            expr,
            input: Box::new(self.plan.clone())
        })
    }

    fn filter(&self, expr: LogicalExpr) -> LogicalPlanBuilder {
        LogicalPlanBuilder::from(LogicalPlan::Selection {
            expr: Box::new(expr),
            input: Box::new(self.plan.clone())
        })
    }

    fn build(&self) -> LogicalPlan {
        self.plan.clone()
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
    use super::LogicalExpr::*;
    use crate::logical_plan::LogicalPlanBuilder;

    #[test]
    fn build_plan_manually() {

        let scan = Scan {
            filename: "employee.csv".to_owned()
        };

        let plan = LogicalPlanBuilder::from(scan)
            .filter(eq(col("state"), lit_str("CO")))
            .project(vec![col("state")])
            .build();

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
    }

}