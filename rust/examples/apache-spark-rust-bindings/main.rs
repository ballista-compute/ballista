use std::collections::HashMap;

use arrow::datatypes::DataType;

extern crate ballista;

use ballista::dataframe::Context;
use ballista::error::Result;
use ballista::logicalplan::{col, Expr};

fn main() -> Result<()> {
    let spark_master = "local[*]";

    let mut spark_settings = HashMap::new();
    spark_settings.insert("spark.app.name", "rust-client-demo");
    spark_settings.insert("spark.executor.memory", "4g");
    spark_settings.insert("spark.executor.cores", "4");

    let ctx = Context::spark(spark_master, spark_settings);

    let df = ctx
        .read_csv("/foo/input.csv", None, None, true)?
        .filter(col("a").lt(&col("b")))?
        .aggregate(vec![col("c")], vec![sum(col("d"))])?;

    df.explain();

    df.write_csv("/foo/output.csv")
}

//TODO move into crate

fn sum(expr: Expr) -> Expr {
    aggregate_expr("SUM", &expr)
}

/// Create an expression to represent a named aggregate function
fn aggregate_expr(name: &str, expr: &Expr) -> Expr {
    let return_type = DataType::Float64;
    Expr::AggregateFunction {
        name: name.to_string(),
        args: vec![expr.clone()],
        return_type,
    }
}
