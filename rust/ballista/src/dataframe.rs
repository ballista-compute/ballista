use std::collections::HashMap;
use std::sync::Arc;


use crate::arrow::datatypes::{DataType, Schema};
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion;
use crate::datafusion::datasource::parquet::ParquetTable;
use crate::datafusion::datasource::TableProvider;
use crate::datafusion::logicalplan::{Expr, LogicalPlan, ScalarValue};
use crate::datafusion::optimizer::utils::exprlist_to_fields;
use crate::client;
use crate::error::{BallistaError, Result};
use crate::plan::Action;

pub const CSV_BATCH_SIZE: &'static str = "ballista.csv.batchSize";

/// Configuration setting
struct ConfigSetting {
    key: String,
    _description: String,
    default_value: Option<String>,
}

impl ConfigSetting {
    pub fn new(key: &str, description: &str, default_value: Option<&str>) -> Self {
        Self {
            key: key.to_owned(),
            _description: description.to_owned(),
            default_value: default_value.map(|s| s.to_owned()),
        }
    }

    pub fn default_value(&self) -> Option<String> {
        self.default_value.clone()
    }
}

struct Configs {
    configs: HashMap<String, ConfigSetting>,
    settings: HashMap<String, String>,
}

impl Configs {
    pub fn new(settings: HashMap<String, String>) -> Self {
        let csv_batch_size: ConfigSetting = ConfigSetting::new(
            CSV_BATCH_SIZE,
            "Number of rows to read per batch",
            Some("1024"),
        );

        let configs = vec![csv_batch_size];

        let mut m = HashMap::new();
        for config in configs {
            m.insert(config.key.clone(), config);
        }

        Self {
            configs: m,
            settings,
        }
    }

    pub fn get_setting(&self, name: &str) -> Option<String> {
        match self.settings.get(name) {
            Some(value) => Some(value.clone()),
            None => match self.configs.get(name) {
                Some(value) => value.default_value(),
                None => None,
            },
        }
    }

    pub fn csv_batch_size(&self) -> Option<String> {
        self.get_setting(CSV_BATCH_SIZE)
    }
}

pub struct Context {
    state: Arc<ContextState>,
}

#[derive(Debug, Clone)]
pub enum ContextState {
    Local {
        settings: HashMap<String, String>,
    },
    Remote {
        host: String,
        port: usize,
        settings: HashMap<String, String>,
    },
    Spark {
        master: String,
        spark_settings: HashMap<String, String>,
    },
}

impl Context {
    /// Create a context for executing a query against a remote Spark executor
    pub fn spark(master: &str, settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState::Spark {
                master: master.to_owned(),
                spark_settings: parse_settings(settings),
            }),
        }
    }

    /// Create a context for executing a query against a local in-process executor
    pub fn local(settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState::Local {
                settings: parse_settings(settings),
            }),
        }
    }

    /// Create a context for executing a query against a remote executor
    pub fn remote(host: &str, port: usize, settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState::Remote {
                host: host.to_owned(),
                port,
                settings: parse_settings(settings),
            }),
        }
    }

    pub fn from(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    /// Create a DataFrame from an existing set of RecordBatch instances
    pub fn create_dataframe(&self, batches: &[RecordBatch]) -> Result<DataFrame> {
        let schema = batches[0].schema().as_ref();
        let plan = LogicalPlan::InMemoryScan {
            data: vec![batches.to_vec()],
            schema: Box::new(schema.clone()),
            projection: None,
            projected_schema: Box::new(schema.clone()),
        };
        Ok(DataFrame::from(self.state.clone(), &plan))
    }

    pub fn read_csv(
        &self,
        path: &str,
        schema: Option<Schema>,
        projection: Option<Vec<usize>>,
        has_header: bool,
    ) -> Result<DataFrame> {
        Ok(DataFrame::scan_csv(
            self.state.clone(),
            path,
            has_header,
            &schema.unwrap(), //TODO schema should be optional here
            projection,
        )?)
    }

    pub fn read_parquet(&self, path: &str, projection: Option<Vec<usize>>) -> Result<DataFrame> {
        Ok(DataFrame::scan_parquet(
            self.state.clone(),
            path,
            projection,
        )?)
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

fn parse_settings(settings: HashMap<&str, &str>) -> HashMap<String, String> {
    let mut s: HashMap<String, String> = HashMap::new();
    for (k, v) in settings {
        s.insert(k.to_owned(), v.to_owned());
    }
    s
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
        has_header: bool,
        schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        Ok(Self::from(
            ctx,
            &LogicalPlan::CsvScan {
                path: path.to_owned(),
                has_header,
                delimiter: None,
                schema: Box::new(schema.clone()),
                projected_schema: Box::new(projected_schema.or(Some(schema.clone())).unwrap()),
                projection,
            },
        ))
    }

    /// Scan a data source
    pub fn scan_parquet(
        ctx: Arc<ContextState>,
        path: &str,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema().as_ref().to_owned();
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));

        Ok(Self::from(
            ctx,
            &LogicalPlan::ParquetScan {
                path: path.to_owned(),
                schema: Box::new(schema.clone()),
                projection,
                projected_schema: Box::new(projected_schema.or(Some(schema.clone())).unwrap()),
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
                schema: Box::new(schema.clone()),
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

        match &self.ctx_state.as_ref() {
            ContextState::Spark { spark_settings, .. } => {
                let host = &spark_settings["spark.ballista.host"];
                let port = &spark_settings["spark.ballista.port"];
                ctx.execute_action(host, port.parse::<usize>().unwrap(), action)
                    .await
            }
            ContextState::Remote { host, port, .. } => {
                ctx.execute_action(host, *port, action).await
            }
            ContextState::Local { settings } => {
                // create local execution context
                let mut ctx = datafusion::execution::context::ExecutionContext::new();

                let datafusion_plan = &self.plan;

                // create the query plan
                let optimized_plan = ctx.optimize(&datafusion_plan)?;

                println!("Optimized Plan: {:?}", optimized_plan);

                let x = Configs::new(settings.clone());

                let batch_size = x.csv_batch_size().unwrap().parse::<usize>().unwrap();

                println!("batch_size={}", batch_size);

                let physical_plan = ctx.create_physical_plan(&optimized_plan, batch_size)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_context_ux() {
        let mut settings = HashMap::new();
        settings.insert(CSV_BATCH_SIZE, "2048");
        settings.insert("custom.setting", "/foo/bar");

        let _ = Context::local(settings);
    }
}
