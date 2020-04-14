
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::logicalplan::{Expr, ScalarValue};
use datafusion::optimizer::utils;

use crate::dataframe::DataFrame;
use crate::error::{Result, BallistaError};

use std::fmt;
use std::collections::HashMap;

pub struct Context {

}

impl Context {

    pub fn new() -> Self {
        Self {}
    }

    pub fn spark(master: &str, settings: HashMap<&str, &str>) -> Self {
        Self {}
    }

    pub fn read_csv(&self, path: &str, schema: Option<Schema>, projection: Option<Vec<usize>>, has_header: bool) -> Result<Box<dyn DataFrame>> {
        Ok(Box::new(DataFrameImpl::scan_csv(path, &schema.unwrap(), projection)?) as Box<dyn DataFrame>)
    }

    pub fn read_parquet(&self, path: &str) -> Result<Box<dyn DataFrame>> {
        unimplemented!()
    }

}

/// Builder for logical plans
pub struct DataFrameImpl {
    plan: LogicalPlan,
}

impl DataFrameImpl {

    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation
    pub fn empty() -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            schema: Box::new(Schema::empty()),
        })
    }

    /// Scan a data source
    pub fn scan_csv(
        path: &str,
        schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = projection.clone().map(|p| {
            Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect())
        });
        Ok(Self::from(&LogicalPlan::FileScan {
            path: path.to_owned(),
            schema: Box::new(schema.clone()),
            projected_schema: Box::new(
                projected_schema.or(Some(schema.clone())).unwrap(),
            ),
            projection,
        }))
    }


}

impl DataFrame for DataFrameImpl {
    /// Apply a projection
    fn project(&self, expr: Vec<Expr>) -> Result<Box<dyn DataFrame>> {
        let input_schema = self.plan.schema();
        let projected_expr = if expr.contains(&Expr::Wildcard) {
            let mut expr_vec = vec![];
            (0..expr.len()).for_each(|i| match &expr[i] {
                Expr::Wildcard => {
                    (0..input_schema.fields().len())
                        .for_each(|i| expr_vec.push(Expr::Column(i).clone()));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema = Schema::new(utils::exprlist_to_fields(
            &projected_expr,
            input_schema.as_ref(),
        )?);

        let df = Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Box::new(self.plan.clone()),
            schema: Box::new(schema),
        });

        Ok(Box::new(df))
    }

    /// Apply a filter
    fn filter(&self, expr: Expr) -> Result<Box<dyn DataFrame>> {
        Ok(Box::new(Self::from(&LogicalPlan::Selection {
            expr,
            input: Box::new(self.plan.clone()),
        })))
    }

    /// Apply a limit
    fn limit(&self, n: usize) -> Result<Box<dyn DataFrame>> {
        Ok(Box::new(Self::from(&LogicalPlan::Limit {
            expr: Expr::Literal(ScalarValue::UInt64(n as u64)),
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        })))
    }

    /// Apply an aggregate
    fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Box<dyn DataFrame>> {
        let mut all_fields: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

        let aggr_schema =
            Schema::new(utils::exprlist_to_fields(&all_fields, self.plan.schema())?);

        Ok(Box::new(Self::from(&LogicalPlan::Aggregate {
            input: Box::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: Box::new(aggr_schema),
        })))
    }



    fn collect(&self) -> Result<Vec<RecordBatch>> {
        unimplemented!()
    }

    fn write_csv(&self, path: &str) -> Result<()> {
        unimplemented!()
    }

    fn write_parquet(&self, path: &str) -> Result<()> {
        unimplemented!()
    }

    fn schema(&self) -> Box<Schema> {
        unimplemented!()
    }

    fn explain(&self) {
        unimplemented!()
    }
}

/// The LogicalPlan represents different types of relations (such as Projection,
/// Selection, etc) and can be created by the SQL query planner and the DataFrame API.
#[derive(Clone)]
pub enum LogicalPlan {
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
    },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    FileScan {
        /// The path to the files
        path: String,
        /// The underlying table schema
        schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The projected schema
        projected_schema: Box<Schema>,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The expression
        expr: Expr,
        /// The logical plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Box<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::FileScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
        }
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
            LogicalPlan::FileScan {
                path: ref table_name,
                ref projection,
                ..
            } => write!(f, "TableScan: {} projection={:?}", table_name, projection),
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                write!(
                    f,
                    "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                    group_expr, aggr_expr
                )?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Limit: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }


}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}
