use arrow::record_batch::RecordBatch;
use datafusion::logicalplan::Expr;
use crate::error::Result;

pub trait DataFrame {

    // transformations

    fn project(&self, expr: Vec<Expr>) -> Result<Box<dyn DataFrame>>;
    fn filter(&self, expr: Expr) -> Result<Box<dyn DataFrame>>;
    fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Box<dyn DataFrame>>;
    fn limit(&self, n: usize) -> Result<Box<dyn DataFrame>>;

    // actions

    fn collect(&self) -> Result<Vec<RecordBatch>>;
    fn write_csv(&self, path: &str) -> Result<()>;
    fn write_parquet(&self, path: &str) -> Result<()>;
}