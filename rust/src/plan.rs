use datafusion::logicalplan::LogicalPlan;
use arrow::datatypes::Schema;

#[derive(Debug,Clone)]
pub enum Action {
    RemoteQuery {
        plan: LogicalPlan,
        tables: Vec<TableMeta>
    },
}

#[derive(Debug,Clone)]
pub enum TableMeta {
    Csv {
        table_name: String,
        path: String,
        has_header: bool,
        schema: Schema
    },
    Parquet {
        table_name: String,
        path: String,
    },
}