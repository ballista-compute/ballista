use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};

extern crate ballista;

use ballista::dataframe::*;
use ballista::error::Result;
use ballista::logicalplan::col;
use datafusion::utils;

#[tokio::main]
async fn main() -> Result<()> {
    let spark_master = "local[*]";

    let mut spark_settings = HashMap::new();
    spark_settings.insert("spark.app.name", "rust-client-demo");
    spark_settings.insert("spark.ballista.host", "localhost");
    spark_settings.insert("spark.ballista.port", "50051");
    spark_settings.insert("spark.executor.memory", "4g");
    spark_settings.insert("spark.executor.cores", "4");

    let ctx = Context::spark(spark_master, spark_settings);

    let path = "/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv";

    let df = ctx
        .read_csv(path, Some(nyctaxi_schema()), None, true)?
        //.filter(col("passenger_count").gt(&lit))?
        .aggregate(
            vec![col("passenger_count")],
            vec![min(col("fare_amount")), max(col("fare_amount"))],
        )?;

    // print the query plan
    df.explain();

    // collect the results from the Spark executor
    let results = df.collect().await?;

    // display the results
    utils::print_batches(&results)?;

    Ok(())
}

fn nyctaxi_schema() -> Schema {
    Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Int32, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ])
}
