use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};

extern crate ballista;

use ballista::dataframe::{max, min, Context};
use ballista::error::Result;
use ballista::logicalplan::*;
use ballista::BALLISTA_VERSION;

use arrow::array::{Float64Array, Int32Array};
use datafusion::utils::print_batches;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Ballista v{} Rust Integration Tests", BALLISTA_VERSION);

    let filename = "/mnt/nyctaxi/yellow_tripdata_2019-01.csv";

    let host = "localhost";

    let mut targets = HashMap::new();
    targets.insert("Rust", 50051);
    targets.insert("JVM", 50052);
    targets.insert("Spark", 50053);

    for (name, port) in &targets {
        println!(
            "Executing query against {} executor at {}:{}",
            name, host, port
        );
        let start = Instant::now();
        let ctx = Context::remote(host, *port);
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
        print_batches(&response)?;

        // assertions
        assert_eq!(1, response.len());
        let batch = &response[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(10, batch.num_rows());

        let _passenger_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column 0 incorrect data type");
        let _min_fare = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("column 1 incorrect data type");
        let _max_fare = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("column 2 incorrect data type");

        //TODO more assertions to compare the results from each target
    }

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
