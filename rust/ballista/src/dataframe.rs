// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::arrow::datatypes::{DataType, Schema};
use crate::arrow::record_batch::RecordBatch;
use crate::client;
use crate::datafusion;
pub use crate::datafusion::datasource::csv::CsvReadOptions;
use crate::datafusion::datasource::parquet::ParquetTable;
use crate::datafusion::datasource::MemTable;
use crate::datafusion::datasource::TableProvider;
use crate::datafusion::logicalplan::{
    Expr, FunctionMeta, LogicalPlan, LogicalPlanBuilder, ScalarValue,
};
use crate::datafusion::optimizer::utils::exprlist_to_fields;
use crate::datafusion::sql::parser::{DFASTNode, DFParser};
use crate::datafusion::sql::planner::{SchemaProvider, SqlToRel};
use crate::error::{BallistaError, Result};
use crate::logical_plan::Action;

pub const CSV_BATCH_SIZE: &str = "ballista.csv.batchSize";

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

#[derive(Debug)]
pub struct ContextSchemaProvider {
    pub temp_tables: HashMap<String, DataFrame>,
}

impl ContextSchemaProvider {
    fn new() -> Self {
        Self {
            temp_tables: HashMap::new(),
        }
    }
}

impl ContextSchemaProvider {
    pub fn register_temp_table(&mut self, name: &str, df: DataFrame) -> Result<()> {
        self.temp_tables.insert(name.to_string(), df);
        Ok(())
    }
}

impl SchemaProvider for &ContextSchemaProvider {
    fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>> {
        self.temp_tables
            .get(name)
            .map(|df| Arc::from(df.plan.schema().clone()))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<FunctionMeta>> {
        // TODO: support udf
        None
    }
}

#[derive(Debug)]
pub struct Context {
    pub state: Arc<ContextState>,
}

#[derive(Debug)]
pub struct ContextState {
    pub schema_provider: RwLock<ContextSchemaProvider>,
    pub backend: ContextBackend,
}

#[derive(Debug, Clone)]
pub enum ContextBackend {
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
            state: Arc::new(ContextState {
                schema_provider: RwLock::new(ContextSchemaProvider::new()),
                backend: ContextBackend::Spark {
                    master: master.to_owned(),
                    spark_settings: parse_settings(settings),
                },
            }),
        }
    }

    /// Create a context for executing a query against a local in-process executor
    pub fn local(settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState {
                schema_provider: RwLock::new(ContextSchemaProvider::new()),
                backend: ContextBackend::Local {
                    settings: parse_settings(settings),
                },
            }),
        }
    }

    /// Create a context for executing a query against a remote executor
    pub fn remote(host: &str, port: usize, settings: HashMap<&str, &str>) -> Self {
        Self {
            state: Arc::new(ContextState {
                schema_provider: RwLock::new(ContextSchemaProvider::new()),
                backend: ContextBackend::Remote {
                    host: host.to_owned(),
                    port,
                    settings: parse_settings(settings),
                },
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
        Ok(DataFrame::from(self.state.clone(), plan))
    }

    pub fn read_csv(
        &self,
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
    ) -> Result<DataFrame> {
        Ok(DataFrame::scan_csv(
            self.state.clone(),
            path,
            options,
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

    pub fn sql(&self, sql: &str) -> Result<DataFrame> {
        let ast = DFParser::parse_sql(sql)?;
        match ast {
            DFASTNode::ANSI(ansi) => {
                let plan = SqlToRel::new(&*self.state.schema_provider.read().unwrap())
                    .sql_to_rel(&ansi)?;
                Ok(DataFrame::from(self.state.clone(), plan))
            }
            DFASTNode::CreateExternalTable { .. } => {
                unimplemented!("TODO");
            }
        }
    }

    pub fn register_temp_table(&mut self, name: &str, df: DataFrame) -> Result<()> {
        let mut provider = self.state.schema_provider.write().unwrap();
        provider.register_temp_table(name, df)?;
        Ok(())
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
#[derive(Clone, Debug)]
pub struct DataFrame {
    ctx_state: Arc<ContextState>,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a builder from an existing plan
    pub fn from(ctx_state: Arc<ContextState>, plan: LogicalPlan) -> Self {
        Self { ctx_state, plan }
    }

    /// Create an empty relation
    pub fn empty(ctx_state: Arc<ContextState>) -> Self {
        Self::from(
            ctx_state,
            LogicalPlan::EmptyRelation {
                schema: Box::new(Schema::empty()),
            },
        )
    }

    /// Scan a data source
    pub fn scan_csv(
        ctx_state: Arc<ContextState>,
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self::from(
            ctx_state,
            LogicalPlanBuilder::scan_csv(path, options, projection)?.build()?,
        ))
    }

    /// Scan a data source
    pub fn scan_parquet(
        ctx_state: Arc<ContextState>,
        path: &str,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema().as_ref().to_owned();
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));

        Ok(Self::from(
            ctx_state,
            LogicalPlan::ParquetScan {
                path: path.to_owned(),
                schema: Box::new(schema.clone()),
                projection,
                projected_schema: Box::new(projected_schema.or(Some(schema)).unwrap()),
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
                    (0..input_schema.fields().len()).for_each(|i| expr_vec.push(Expr::Column(i)));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr
        };

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema)?);

        let df = Self::from(
            self.ctx_state.clone(),
            LogicalPlan::Projection {
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
            LogicalPlan::Selection {
                expr,
                input: Box::new(self.plan.clone()),
            },
        ))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<DataFrame> {
        Ok(Self::from(
            self.ctx_state.clone(),
            LogicalPlan::Sort {
                expr,
                input: Box::new(self.plan.clone()),
                schema: self.plan.schema().clone(),
            },
        ))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<DataFrame> {
        Ok(Self::from(
            self.ctx_state.clone(),
            LogicalPlan::Limit {
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
            LogicalPlan::Aggregate {
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

    fn datafusion_collect(
        &self,
        ctx: &datafusion::execution::context::ExecutionContext,
        settings: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>> {
        let datafusion_plan = &self.plan;

        println!("PreOptimized Plan: {:?}", datafusion_plan);
        // create the query plan
        let optimized_plan = ctx.optimize(&datafusion_plan)?;

        println!("Optimized Plan: {:?}", optimized_plan);

        let batch_size = Configs::new(settings.clone())
            .csv_batch_size()
            .unwrap()
            .parse::<usize>()
            .unwrap();

        println!("batch_size={}", batch_size);

        let physical_plan = ctx.create_physical_plan(&optimized_plan, batch_size)?;
        println!("physicalplan schema: {:#?}", physical_plan.schema());

        // execute the query
        ctx.collect(physical_plan.as_ref())
            .map_err(BallistaError::DataFusionError)
    }

    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let action = Action::Collect {
            plan: self.plan.clone(),
        };

        match &self.ctx_state.backend {
            ContextBackend::Spark { spark_settings, .. } => {
                let host = &spark_settings["spark.ballista.host"];
                let port = &spark_settings["spark.ballista.port"];
                Context::from(self.ctx_state.clone())
                    .execute_action(host, port.parse::<usize>().unwrap(), action)
                    .await
            }
            ContextBackend::Remote { host, port, .. } => {
                Context::from(self.ctx_state.clone())
                    .execute_action(host, *port, action)
                    .await
            }
            ContextBackend::Local { settings } => {
                // create local execution context
                let mut datafusion_ctx = datafusion::execution::context::ExecutionContext::new();

                for (name, df) in &self.ctx_state.schema_provider.read().unwrap().temp_tables {
                    let partitions = vec![df.datafusion_collect(&datafusion_ctx, &settings)?];
                    let mem_tbl = MemTable::new(Arc::new(*df.plan.schema().clone()), partitions)?;
                    datafusion_ctx.register_table(name, Box::new(mem_tbl));
                }

                self.datafusion_collect(&datafusion_ctx, &settings)
            }
        }
    }

    #[allow(clippy::match_single_binding)]
    pub fn write_csv(&self, _path: &str) -> Result<()> {
        match &self.ctx_state.backend {
            other => Err(BallistaError::NotImplemented(format!(
                "write_csv() is not implemented for {:?} yet",
                other
            ))),
        }
    }

    #[allow(clippy::match_single_binding)]
    pub fn write_parquet(&self, _path: &str) -> Result<()> {
        match &self.ctx_state.backend {
            other => Err(BallistaError::NotImplemented(format!(
                "write_parquet() is not implemented for {:?} yet",
                other
            ))),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
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

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expr {
    Expr::UnresolvedColumn(name.to_owned())
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
    use crate::arrow::array::*;
    use crate::arrow::datatypes::*;

    #[test]
    fn create_context_ux() {
        let mut settings = HashMap::new();
        settings.insert(CSV_BATCH_SIZE, "2048");
        settings.insert("custom.setting", "/foo/bar");

        let _ = Context::local(settings);
    }

    #[tokio::test]
    async fn sql_ux() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batches = vec![RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )?];

        let mut ctx = Context::local(HashMap::new());

        let df = ctx
            .create_dataframe(&batches)?
            .project(vec![Expr::Wildcard])?;
        ctx.register_temp_table("df", df)?;

        let result = ctx.sql("SELECT * FROM df WHERE a > 3")?.collect().await?;

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(0).data(), Int32Array::from(vec![4, 5]).data(),);
        assert_eq!(
            batch.column(1).data(),
            StringArray::from(vec!["d", "e"]).data(),
        );
        Ok(())
    }
}
