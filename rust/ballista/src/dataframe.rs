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

//! The DataFrame API is the main entry point into Ballista.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::arrow::datatypes::{DataType, Schema};
use crate::arrow::record_batch::RecordBatch;
pub use crate::datafusion::datasource::csv::CsvReadOptions;
use crate::datafusion::datasource::parquet::ParquetTable;
use crate::datafusion::datasource::TableProvider;
use crate::datafusion::logicalplan::ScalarValue;
use crate::datafusion::logicalplan::{exprlist_to_fields, Operator};
use crate::datafusion::logicalplan::{Expr, FunctionMeta, LogicalPlan, LogicalPlanBuilder};
use crate::datafusion::sql::parser::DFParser;
use crate::datafusion::sql::planner::{SchemaProvider, SqlToRel};
use crate::distributed::client;
use crate::error::{ballista_error, BallistaError, Result};
use crate::execution::physical_plan::Action;

pub const CSV_READER_BATCH_SIZE: &str = "ballista.csv.reader.batchSize";
pub const PARQUET_READER_BATCH_SIZE: &str = "ballista.parquet.reader.batchSize";

/// Configuration setting
#[derive(Debug, Clone)]
struct ConfigSetting {
    key: String,
    description: String,
    data_type: DataType,
    default_value: Option<String>,
}

impl ConfigSetting {
    pub fn new(
        key: &str,
        description: &str,
        data_type: DataType,
        default_value: Option<&str>,
    ) -> Self {
        Self {
            key: key.to_owned(),
            description: description.to_owned(),
            data_type,
            default_value: default_value.map(|s| s.to_owned()),
        }
    }
}

struct BallistaConfigs {
    configs: HashMap<String, ConfigSetting>,
}

impl BallistaConfigs {
    pub fn new() -> Self {
        let mut configs = vec![];

        configs.push(ConfigSetting::new(
            CSV_READER_BATCH_SIZE,
            "Number of rows to read per batch",
            DataType::UInt64,
            Some("65536"),
        ));

        configs.push(ConfigSetting::new(
            PARQUET_READER_BATCH_SIZE,
            "Number of rows to read per batch",
            DataType::UInt64,
            Some("65536"),
        ));

        let mut config_map: HashMap<String, ConfigSetting> = HashMap::new();
        for config in &configs {
            config_map.insert(config.key.to_owned(), config.to_owned());
        }

        Self {
            configs: config_map,
        }
    }

    pub fn validate(&self, settings: &HashMap<String, String>) -> Result<HashMap<String, String>> {
        let mut checked_settings = settings.clone();

        for config in &self.configs {
            if !checked_settings.contains_key(config.0) {
                match &config.1.default_value {
                    Some(default_value) => {
                        checked_settings.insert(config.0.to_string(), default_value.to_string());
                    }
                    None => {
                        return Err(ballista_error(&format!(
                            "Settings missing required config '{}'",
                            config.0
                        )))
                    }
                }
            }

            // validate that any values are of the expected type
            if let Some(value) = checked_settings.get(config.0) {
                match config.1.data_type {
                    DataType::UInt64 => {
                        let _ = value.parse::<u64>().map_err(|e| {
                            ballista_error(&format!(
                                "Error parsing value {} for setting '{}' {:?}",
                                value, config.0, e
                            ))
                        })?;
                    }
                    _ => return Err(ballista_error("unsupported data type for configs")),
                }
            }
        }

        Ok(checked_settings)
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
        let schema = batches[0].schema();
        let plan = LogicalPlan::InMemoryScan {
            data: vec![batches.to_vec()],
            schema: Box::new(schema.as_ref().clone()),
            projection: None,
            projected_schema: Box::new(schema.as_ref().clone()),
        };
        Ok(DataFrame::from(self.state.clone(), plan))
    }

    pub fn read_csv(&self, path: &str, options: CsvReadOptions) -> Result<DataFrame> {
        Ok(DataFrame::scan_csv(self.state.clone(), path, options)?)
    }

    pub fn read_parquet(&self, path: &str) -> Result<DataFrame> {
        Ok(DataFrame::scan_parquet(self.state.clone(), path)?)
    }

    pub fn sql(&self, sql: &str) -> Result<DataFrame> {
        let statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(BallistaError::NotImplemented(
                "The dataframe currently only supports a single SQL statement".to_string(),
            ));
        }

        let plan = SqlToRel::new(&*self.state.schema_provider.read().unwrap())
            .statement_to_plan(&statements[0])?;
        Ok(DataFrame::from(self.state.clone(), plan))
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
        client::execute_action(host, port, &action).await
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
    ) -> Result<Self> {
        Ok(Self::from(
            ctx_state,
            LogicalPlanBuilder::scan_csv(path, options, None)?.build()?,
        ))
    }

    /// Scan a data source
    pub fn scan_parquet(ctx_state: Arc<ContextState>, path: &str) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema().as_ref().to_owned();
        Ok(Self::from(
            ctx_state,
            LogicalPlan::ParquetScan {
                path: path.to_owned(),
                schema: Box::new(schema.clone()),
                projection: None,
                projected_schema: Box::new(schema),
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
                    input_schema
                        .fields()
                        .iter()
                        .for_each(|f| expr_vec.push(Expr::Column(f.name().clone())));
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
            },
        ))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<DataFrame> {
        Ok(Self::from(
            self.ctx_state.clone(),
            LogicalPlan::Limit {
                n,
                input: Box::new(self.plan.clone()),
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

    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        match &self.ctx_state.backend {
            ContextBackend::Spark { spark_settings, .. } => {
                let host = &spark_settings["spark.ballista.host"];
                let port = &spark_settings["spark.ballista.port"];

                let action = Action::InteractiveQuery {
                    plan: self.plan.clone(),
                    settings: spark_settings.clone(),
                };

                Context::from(self.ctx_state.clone())
                    .execute_action(host, port.parse::<usize>().unwrap(), action)
                    .await
            }
            ContextBackend::Remote {
                host,
                port,
                settings,
            } => {
                let configs = BallistaConfigs::new();
                let settings = configs.validate(settings)?;

                let action = Action::InteractiveQuery {
                    plan: self.plan.clone(),
                    settings,
                };

                Context::from(self.ctx_state.clone())
                    .execute_action(host, *port, action)
                    .await
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
pub fn avg(expr: Expr) -> Expr {
    aggregate_expr("AVG", &expr)
}
pub fn count(expr: Expr) -> Expr {
    aggregate_expr("COUNT", &expr)
}

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_owned())
}

pub fn alias(expr: Expr, name: &str) -> Expr {
    Expr::Alias(Box::new(expr), name.to_owned())
}

pub fn add(l: Expr, r: Expr) -> Expr {
    binary_expr(l, Operator::Plus, r)
}

pub fn subtract(l: Expr, r: Expr) -> Expr {
    binary_expr(l, Operator::Minus, r)
}

pub fn mult(l: Expr, r: Expr) -> Expr {
    binary_expr(l, Operator::Multiply, r)
}

pub fn div(l: Expr, r: Expr) -> Expr {
    binary_expr(l, Operator::Divide, r)
}

fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// Create a literal string expression
pub fn lit_str(str: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(str.to_owned()))
}

/// Create a literal i8 expression
pub fn lit_i8(n: i8) -> Expr {
    Expr::Literal(ScalarValue::Int8(n))
}

/// Create a literal i16 expression
pub fn lit_i16(n: i16) -> Expr {
    Expr::Literal(ScalarValue::Int16(n))
}

/// Create a literal i32 expression
pub fn lit_i32(n: i32) -> Expr {
    Expr::Literal(ScalarValue::Int32(n))
}

/// Create a literal i64 expression
pub fn lit_i64(n: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(n))
}

/// Create a literal u8 expression
pub fn lit_u8(n: u8) -> Expr {
    Expr::Literal(ScalarValue::UInt8(n))
}

/// Create a literal u16 expression
pub fn lit_u16(n: u16) -> Expr {
    Expr::Literal(ScalarValue::UInt16(n))
}

/// Create a literal u32 expression
pub fn lit_u32(n: u32) -> Expr {
    Expr::Literal(ScalarValue::UInt32(n))
}

/// Create a literal u64 expression
pub fn lit_u64(n: u64) -> Expr {
    Expr::Literal(ScalarValue::UInt64(n))
}

/// Create a literal f32 expression
pub fn lit_f32(n: f32) -> Expr {
    Expr::Literal(ScalarValue::Float32(n))
}

/// Create a literal f64 expression
pub fn lit_f64(n: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(n))
}

/// Create an expression to represent a named aggregate function
pub fn aggregate_expr(name: &str, expr: &Expr) -> Expr {
    Expr::AggregateFunction {
        name: name.to_string(),
        args: vec![expr.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_settings() -> Result<()> {
        let my_settings = HashMap::new();
        let configs = BallistaConfigs::new();
        let my_settings = configs.validate(&my_settings)?;
        assert_eq!(my_settings[PARQUET_READER_BATCH_SIZE], "65536");
        Ok(())
    }

    #[test]
    fn custom_setting() -> Result<()> {
        let mut my_settings: HashMap<String, String> = HashMap::new();
        my_settings.insert(PARQUET_READER_BATCH_SIZE.to_owned(), "1234".to_owned());
        let configs = BallistaConfigs::new();
        let my_settings = configs.validate(&my_settings)?;
        assert_eq!(my_settings[PARQUET_READER_BATCH_SIZE], "1234");
        Ok(())
    }

    #[test]
    fn invalid_setting() -> Result<()> {
        let mut my_settings: HashMap<String, String> = HashMap::new();
        my_settings.insert(
            PARQUET_READER_BATCH_SIZE.to_owned(),
            "twenty gigs".to_owned(),
        );
        let configs = BallistaConfigs::new();
        match configs.validate(&my_settings) {
            Err(e) => assert_eq!("General error: Error parsing value twenty gigs for setting 'ballista.parquet.reader.batchSize' ParseIntError { kind: InvalidDigit }", e.to_string()),
            _ => return Err(ballista_error("validation failed"))
        }
        Ok(())
    }
}
