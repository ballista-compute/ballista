use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use datafusion;

use crate::client;
use crate::error::{BallistaError, Result};
use crate::logicalplan::{exprlist_to_fields, Expr, LogicalPlan, Operator, ScalarValue};

use std::collections::HashMap;
use std::sync::Arc;

use crate::plan::Action;
use datafusion::datasource::MemTable;

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
            ContextState::Local => {
                // create local execution context
                let mut ctx = datafusion::execution::context::ExecutionContext::new();

                let datafusion_plan = translate_plan(&mut ctx, &self.plan)?;

                // create the query plan
                let optimized_plan = ctx.optimize(&datafusion_plan)?;

                println!("Optimized Plan: {:?}", optimized_plan);

                let physical_plan = ctx.create_physical_plan(&optimized_plan, 1024 * 1024)?;

                // execute the query
                ctx.collect(physical_plan.as_ref())
                    .map_err(|e| BallistaError::DataFusionError(e))
            }
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

/// Translate Ballista plan to DataFusion plan
pub fn translate_plan(
    ctx: &mut datafusion::execution::context::ExecutionContext,
    plan: &LogicalPlan,
) -> Result<datafusion::logicalplan::LogicalPlan> {
    match plan {
        LogicalPlan::MemoryScan(batches) => {
            let table_name = "df_t0"; //TODO generate unique table name
            let schema = (&batches[0]).schema().as_ref();
            let provider = MemTable::new(Arc::new(schema.clone()), batches.clone())?;
            ctx.register_table(table_name, Box::new(provider));
            Ok(datafusion::logicalplan::LogicalPlan::TableScan {
                schema_name: "default".to_owned(),
                table_name: table_name.to_owned(),
                table_schema: Arc::new(schema.clone()),
                projected_schema: Arc::new(schema.clone()),
                projection: None,
            })
        }
        LogicalPlan::FileScan {
            path,
            schema,
            projection,
            projected_schema,
        } => {
            //TODO generate unique table name
            let table_name = "tbd".to_owned();

            ctx.register_csv(&table_name, path.as_str(), schema, true);

            Ok(datafusion::logicalplan::LogicalPlan::TableScan {
                schema_name: "default".to_owned(),
                table_name: table_name.clone(),
                table_schema: Arc::new(schema.clone()),
                projected_schema: Arc::new(projected_schema.clone()),
                projection: projection.clone(),
            })
        }
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => Ok(datafusion::logicalplan::LogicalPlan::Projection {
            expr: expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            input: Arc::new(translate_plan(ctx, input)?),
            schema: Arc::new(schema.clone()),
        }),
        LogicalPlan::Selection { expr, input } => {
            Ok(datafusion::logicalplan::LogicalPlan::Selection {
                expr: translate_expr(expr)?,
                input: Arc::new(translate_plan(ctx, input)?),
            })
        }
        LogicalPlan::Aggregate {
            group_expr,
            aggr_expr,
            input,
            schema,
        } => Ok(datafusion::logicalplan::LogicalPlan::Aggregate {
            group_expr: group_expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            aggr_expr: aggr_expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            input: Arc::new(translate_plan(ctx, input)?),
            schema: Arc::new(schema.clone()),
        }),
        other => Err(BallistaError::General(format!(
            "Cannot translate operator to DataFusion: {:?}",
            other
        ))),
    }
}

/// Translate Ballista expression to DataFusion expression
fn translate_expr(expr: &Expr) -> Result<datafusion::logicalplan::Expr> {
    match expr {
        Expr::Alias(expr, alias) => Ok(datafusion::logicalplan::Expr::Alias(
            Arc::new(translate_expr(expr.as_ref())?),
            alias.clone(),
        )),
        Expr::Column(index) => Ok(datafusion::logicalplan::Expr::Column(*index)),
        Expr::UnresolvedColumn(name) => Ok(datafusion::logicalplan::Expr::UnresolvedColumn(
            name.clone(),
        )),
        Expr::Literal(value) => {
            let value = translate_scalar_value(value)?;
            Ok(datafusion::logicalplan::Expr::Literal(value.clone()))
        }
        Expr::BinaryExpr { left, op, right } => {
            let left = translate_expr(left)?;
            let right = translate_expr(right)?;
            let op = translate_operator(op)?;
            Ok(datafusion::logicalplan::Expr::BinaryExpr {
                left: Arc::new(left),
                op,
                right: Arc::new(right),
            })
        }
        Expr::AggregateFunction {
            name,
            args,
            return_type,
        } => {
            let args = args
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(datafusion::logicalplan::Expr::AggregateFunction {
                name: name.to_owned(),
                args,
                return_type: return_type.clone(),
            })
        }
        other => Err(BallistaError::General(format!(
            "Cannot translate expression to DataFusion: {:?}",
            other
        ))),
    }
}

fn translate_operator(op: &Operator) -> Result<datafusion::logicalplan::Operator> {
    match op {
        Operator::Eq => Ok(datafusion::logicalplan::Operator::Eq),
        Operator::NotEq => Ok(datafusion::logicalplan::Operator::NotEq),
        Operator::Lt => Ok(datafusion::logicalplan::Operator::Lt),
        Operator::LtEq => Ok(datafusion::logicalplan::Operator::LtEq),
        Operator::Gt => Ok(datafusion::logicalplan::Operator::Gt),
        Operator::GtEq => Ok(datafusion::logicalplan::Operator::GtEq),
        Operator::And => Ok(datafusion::logicalplan::Operator::And),
        Operator::Or => Ok(datafusion::logicalplan::Operator::Or),
        Operator::Plus => Ok(datafusion::logicalplan::Operator::Plus),
        Operator::Minus => Ok(datafusion::logicalplan::Operator::Minus),
        Operator::Multiply => Ok(datafusion::logicalplan::Operator::Multiply),
        Operator::Divide => Ok(datafusion::logicalplan::Operator::Divide),
        Operator::Like => Ok(datafusion::logicalplan::Operator::Like),
        Operator::NotLike => Ok(datafusion::logicalplan::Operator::NotLike),
        Operator::Modulus => Ok(datafusion::logicalplan::Operator::Modulus),
        other => Err(BallistaError::General(format!(
            "Cannot translate binary operator to DataFusion: {:?}",
            other
        ))),
    }
}

fn translate_scalar_value(value: &ScalarValue) -> Result<datafusion::logicalplan::ScalarValue> {
    match value {
        ScalarValue::Boolean(v) => Ok(datafusion::logicalplan::ScalarValue::Boolean(*v)),
        ScalarValue::UInt8(v) => Ok(datafusion::logicalplan::ScalarValue::UInt8(*v)),
        ScalarValue::UInt16(v) => Ok(datafusion::logicalplan::ScalarValue::UInt16(*v)),
        ScalarValue::UInt32(v) => Ok(datafusion::logicalplan::ScalarValue::UInt32(*v)),
        ScalarValue::UInt64(v) => Ok(datafusion::logicalplan::ScalarValue::UInt64(*v)),
        ScalarValue::Int8(v) => Ok(datafusion::logicalplan::ScalarValue::Int8(*v)),
        ScalarValue::Int16(v) => Ok(datafusion::logicalplan::ScalarValue::Int16(*v)),
        ScalarValue::Int32(v) => Ok(datafusion::logicalplan::ScalarValue::Int32(*v)),
        ScalarValue::Int64(v) => Ok(datafusion::logicalplan::ScalarValue::Int64(*v)),
        ScalarValue::Float32(v) => Ok(datafusion::logicalplan::ScalarValue::Float32(*v)),
        ScalarValue::Float64(v) => Ok(datafusion::logicalplan::ScalarValue::Float64(*v)),
        ScalarValue::Utf8(v) => Ok(datafusion::logicalplan::ScalarValue::Utf8(v.clone())),
        other => Err(BallistaError::General(format!(
            "Cannot translate scalar value to DataFusion: {:?}",
            other
        ))),
    }
}
