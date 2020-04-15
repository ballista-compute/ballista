use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::logicalplan::{exprlist_to_fields, Expr, LogicalPlan, ScalarValue};

use std::collections::HashMap;
use std::sync::Arc;

use crate::plan::Action;

pub struct ContextState {
    //TODO: add real things here
    _foo: usize,
}

pub struct Context {
    state: Arc<ContextState>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ContextState { _foo: 0 }),
        }
    }

    pub fn from(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub fn spark(_master: &str, _settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState { _foo: 0 }),
        }
    }

    pub fn read_csv(
        &self,
        path: &str,
        schema: Option<Schema>,
        projection: Option<Vec<usize>>,
        _has_header: bool,
    ) -> Result<DataFrame> {
        Ok(DataFrame::scan_csv(
            self.state.clone(),
            path,
            &schema.unwrap(),
            projection,
        )?)
    }

    pub fn read_parquet(&self, _path: &str) -> Result<DataFrame> {
        unimplemented!()
    }

    pub async fn execute_action(
        &self,
        _host: &str,
        _port: usize,
        _action: Action,
    ) -> Result<Vec<RecordBatch>> {
        //TODO needs to return tokio Stream eventually
        unimplemented!()
    }
}

/// Builder for logical plans
pub struct DataFrame {
    ctx_state: Arc<ContextState>,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a builder from an existing plan
    pub fn from(ctx: Arc<ContextState>, plan: &LogicalPlan) -> Self {
        Self {
            ctx_state: ctx,
            plan: plan.clone(),
        }
    }

    /// Create an empty relation
    pub fn empty(ctx: Arc<ContextState>) -> Self {
        Self::from(
            ctx,
            &LogicalPlan::EmptyRelation {
                schema: Box::new(Schema::empty()),
            },
        )
    }

    /// Scan a data source
    pub fn scan_csv(
        ctx: Arc<ContextState>,
        path: &str,
        schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        Ok(Self::from(
            ctx,
            &LogicalPlan::FileScan {
                path: path.to_owned(),
                schema: Box::new(schema.clone()),
                projected_schema: Box::new(projected_schema.or(Some(schema.clone())).unwrap()),
                projection,
            },
        ))
    }

    /// Apply a projection
    pub fn project(&self, expr: Vec<Expr>) -> Result<DataFrame> {
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

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema.as_ref())?);

        let df = Self::from(
            self.ctx_state.clone(),
            &LogicalPlan::Projection {
                expr: projected_expr,
                input: Box::new(self.plan.clone()),
                schema: Box::new(schema),
            },
        );

        Ok(df)
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<DataFrame> {
        Ok(Self::from(
            self.ctx_state.clone(),
            &LogicalPlan::Selection {
                expr,
                input: Box::new(self.plan.clone()),
            },
        ))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<DataFrame> {
        Ok(Self::from(
            self.ctx_state.clone(),
            &LogicalPlan::Limit {
                expr: Expr::Literal(ScalarValue::UInt64(n as u64)),
                input: Box::new(self.plan.clone()),
                schema: self.plan.schema().clone(),
            },
        ))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<DataFrame> {
        let mut all_fields: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

        let aggr_schema = Schema::new(exprlist_to_fields(&all_fields, self.plan.schema())?);

        Ok(Self::from(
            self.ctx_state.clone(),
            &LogicalPlan::Aggregate {
                input: Box::new(self.plan.clone()),
                group_expr,
                aggr_expr,
                schema: Box::new(aggr_schema),
            },
        ))
    }

    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let ctx = Context::from(self.ctx_state.clone());

        let action = Action::Collect {
            plan: self.plan.clone(),
        };

        ctx.execute_action("", 1234, action).await?;

        unimplemented!()
    }

    pub fn write_csv(&self, _path: &str) -> Result<()> {
        unimplemented!()
    }

    pub fn write_parquet(&self, _path: &str) -> Result<()> {
        unimplemented!()
    }

    pub fn schema(&self) -> Box<Schema> {
        unimplemented!()
    }

    pub fn explain(&self) {
        unimplemented!()
    }
}
