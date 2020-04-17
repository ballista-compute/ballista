use arrow::datatypes::{Schema, DataType};
use arrow::record_batch::RecordBatch;

use crate::client;
use crate::error::Result;
use crate::logicalplan::{exprlist_to_fields, Expr, LogicalPlan, ScalarValue};

use std::collections::HashMap;
use std::sync::Arc;

use crate::plan::Action;

pub struct ContextState {
    settings: HashMap<String, String>,
}

impl ContextState {

    pub fn new(settings: HashMap<&str, &str>) -> Self {
        let mut s: HashMap<String, String> = HashMap::new();
        for (k,v) in settings {
            s.insert(k.to_owned(), v.to_owned());
        }
        Self { settings: s }
    }
}

pub struct Context {
    state: Arc<ContextState>,
}

impl Context {

    pub fn new() -> Self {
        Self {
            state: Arc::new(ContextState::new(HashMap::new()) )
        }
    }

    pub fn from(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub fn spark(_spark_master: &str, spark_settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState::new(spark_settings)),
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
            &schema.unwrap(), //TODO schema should be optional here
            projection,
        )?)
    }

    pub fn read_parquet(&self, _path: &str) -> Result<DataFrame> {
        unimplemented!()
    }

    pub async fn execute_action(
        &self,
        host: &str,
        port: usize,
        action: Action,
    ) -> Result<Vec<RecordBatch>> {

        client::execute_action(host, port, action).await
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

    pub fn explain(&self) {
        println!("{:?}", self.plan);
    }

    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let ctx = Context::from(self.ctx_state.clone());

        let action = Action::Collect {
            plan: self.plan.clone(),
        };

        //TODO should not use spark specific settings to discover executor
        let host = &self.ctx_state.settings["spark.ballista.host"];
        let port = &self.ctx_state.settings["spark.ballista.port"];

        ctx.execute_action(host, port.parse::<usize>().unwrap(), action).await
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

}

pub fn min(expr: Expr) -> Expr {
    aggregate_expr("MIN", &expr)
}

pub fn max(expr: Expr) -> Expr {
    aggregate_expr("MAX", &expr)
}

pub fn sum(expr: Expr) -> Expr {
    aggregate_expr("SUM", &expr)
}

/// Create an expression to represent a named aggregate function
pub fn aggregate_expr(name: &str, expr: &Expr) -> Expr {
    let return_type = DataType::Float64;
    Expr::AggregateFunction {
        name: name.to_string(),
        args: vec![expr.clone()],
        return_type,
    }
}