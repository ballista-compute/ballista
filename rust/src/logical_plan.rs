
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
pub enum LogicalPlan {
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

#[cfg(test)]
mod tests {
    use super::LogicalPlan::*;
    use super::LogicalExpr::*;

    #[test]
    fn build_plan_manually() {

        let scan = Scan {
            filename: "employee.csv".to_owned()
        };

        let filter = Selection {
            input: Box::new(scan),
            expr: Box::new(Eq(
                Box::new(Column("state".to_owned())),
                Box::new(LiteralString("CO".to_owned())),
            ))
        };

        let projection = Projection {
            input: Box::new(filter),
            expr: vec![
                Column("state".to_owned())
            ],
        };

        let plan_str = format!("{:?}", projection);
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