use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::client;
use crate::error::{BallistaError, Result};
use crate::logicalplan::{exprlist_to_fields, Expr, LogicalPlan, ScalarValue};

use std::collections::HashMap;
use std::sync::Arc;

use crate::plan::Action;

pub struct Context {
    state: Arc<ContextState>,
}

#[derive(Debug, Clone)]
pub enum ContextState {
    Local,
    Remote {
        host: String,
        port: usize,
    },
    Spark {
        master: String,
        spark_settings: HashMap<String, String>,
    },
}

impl Context {
    /// Create a context for executing a query against a remote Spark executor
    pub fn spark(master: &str, settings: HashMap<&str, &str>) -> Self {
        let mut s: HashMap<String, String> = HashMap::new();
        for (k, v) in settings {
            s.insert(k.to_owned(), v.to_owned());
        }
        Self {
            state: Arc::new(ContextState::Spark {
                master: master.to_owned(),
                spark_settings: s,
            }),
        }
    }

    /// Create a context for executing a query against a local in-process executor
    pub fn local() -> Self {
        Self {
            state: Arc::new(ContextState::Local),
        }
    }

    /// Create a context for executing a query against a remote executor
    pub fn remote(host: &str, port: usize) -> Self {
        Self {
            state: Arc::new(ContextState::Remote {
                host: host.to_owned(),
                port,
            }),
        }
    }

    pub fn from(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    /// Create a DataFrame from an existing set of RecordBatch instances
    pub fn create_dataframe(&self, batches: &Vec<RecordBatch>) -> Result<DataFrame> {
        let plan = LogicalPlan::MemoryScan(batches.clone());
        Ok(DataFrame::from(self.state.clone(), &plan))
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
                schema: Schema::empty(),
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
                schema: schema.clone(),
                projected_schema: projected_schema.or(Some(schema.clone())).unwrap(),
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

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema)?);

        let df = Self::from(
            self.ctx_state.clone(),
            &LogicalPlan::Projection {
                expr: projected_expr,
                input: Box::new(self.plan.clone()),
                schema,
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
                schema: aggr_schema,
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

        match &self.ctx_state.as_ref() {
            ContextState::Spark { spark_settings, .. } => {
                let host = &spark_settings["spark.ballista.host"];
                let port = &spark_settings["spark.ballista.port"];
                ctx.execute_action(host, port.parse::<usize>().unwrap(), action)
                    .await
            }
            ContextState::Remote { host, port } => ctx.execute_action(host, *port, action).await,
            other => Err(BallistaError::NotImplemented(format!(
                "collect() is not implemented for {:?} yet",
                other
            ))),
        }
    }

    pub fn write_csv(&self, _path: &str) -> Result<()> {
        match &self.ctx_state.as_ref() {
            other => Err(BallistaError::NotImplemented(format!(
                "write_csv() is not implemented for {:?} yet",
                other
            ))),
        }
    }

    pub fn write_parquet(&self, _path: &str) -> Result<()> {
        match &self.ctx_state.as_ref() {
            other => Err(BallistaError::NotImplemented(format!(
                "write_parquet() is not implemented for {:?} yet",
                other
            ))),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.plan.schema()
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
