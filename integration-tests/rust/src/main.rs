use std::collections::HashMap;
use std::time::Instant;

extern crate ballista;

use ballista::arrow::array::{Float64Array, Int32Array};
use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::arrow::util::pretty;
use ballista::datafusion::logicalplan::*;
use ballista::dataframe::{max, min, Context, CSV_BATCH_SIZE};
use ballista::error::Result;
use ballista::BALLISTA_VERSION;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Ballista v{} Rust Integration Tests", BALLISTA_VERSION);

    let filename = "/mnt/nyctaxi/csv/year=2019/yellow_tripdata_2019-01.csv";

    let host = "localhost";

    let mut targets = HashMap::new();
    targets.insert("Rust", 50051);
    // targets.insert("JVM", 50052);
    // targets.insert("Spark", 50053);

    for (name, port) in &targets {
        println!(
            "Executing query against {} executor at {}:{}",
            name, host, port
        );
        match execute(filename, host, name, port).await {
            Ok(_) => println!("{}: OK", name),
            Err(e) => println!("{}: FAIL: {:?}", name, e)
        }
    }

    Ok(())
}

async fn execute(filename: &str, host: &str, name: &&str, port: &usize) -> Result<()> {
    let start = Instant::now();
    let mut settings = HashMap::new();
    settings.insert(CSV_BATCH_SIZE, "1024");
    let ctx = Context::remote(host, *port, settings);
    let response = ctx
        .read_csv(filename, Some(nyctaxi_schema()), None, true)?
        .aggregate(
            vec![col("passenger_count")],
            vec![min(col("fare_amount")), max(col("fare_amount"))],
        )?
        .collect()
        .await?;

    let duration = start.elapsed();
    println!(
        "Executed query against {} executor at {}:{} in {} ms",
        name,
        host,
        port,
        duration.as_millis()
    );
    pretty::print_batches(&response)?;

    // assertions
    assert_eq!(1, response.len());
    let batch = &response[0];
    assert_eq!(3, batch.num_columns());
    assert_eq!(10, batch.num_rows());

    println!("{:?}", &response[0].schema());

    let passenger_count = batch.column(0);
    let min_fare_amt = batch.column(1);
    let max_fare_amt = batch.column(2);

    assert_eq!(&DataType::Int32, passenger_count.data_type());
    assert_eq!(&DataType::Float64, min_fare_amt.data_type());
    assert_eq!(&DataType::Float64, max_fare_amt.data_type());

    let _passenger_count = passenger_count
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column 0 incorrect data type");
    let _min_fare = min_fare_amt
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column 1 incorrect data type");
    let _max_fare = max_fare_amt
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column 2 incorrect data type");

    //TODO more assertions to compare the results from each target

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
