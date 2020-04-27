use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};

extern crate ballista;

use ballista::dataframe::{max, Context};
use ballista::error::Result;
use ballista::logicalplan::*;
use ballista::BALLISTA_VERSION;

use datafusion::utils::print_batches;

#[tokio::main]
async fn main() -> Result<()> {

    println!("Ballista v{} Rust Integration Tests", BALLISTA_VERSION);

    let host = "localhost";
    let port = 50051;

    println!("Executing query against executor at {}:{}", host, port);
    let start = Instant::now();

    let ctx = Context::remote(host, port);

    let filename = "/mnt/nyctaxi/yellow_tripdata_2019-01.csv";

    let response = ctx
        .read_csv(filename, Some(nyctaxi_schema()), None, true)?
        .aggregate(vec![col("passenger_count")], vec![max(col("fare_amount"))])?
        .collect()
        .await?;

    println!(
        "Executed query against executor at {}:{} in {} seconds",
        host,
        port,
        start.elapsed().as_secs()
    );

    print_batches(&response)?;

    //TODO assertions

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
