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
use std::time::Instant;

extern crate ballista;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::arrow::util::pretty;
use ballista::dataframe::{max, min, Context, CSV_READER_BATCH_SIZE};
pub use ballista::datafusion::datasource::csv::CsvReadOptions;
use ballista::datafusion::logicalplan::*;
use ballista::error::BallistaError::General;
use ballista::error::Result;
use ballista::BALLISTA_VERSION;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Ballista v{} Rust Integration Tests", BALLISTA_VERSION);

    let filename = "/mnt/nyctaxi/csv/year=2019/yellow_tripdata_2019-01.csv";

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
        match execute(filename, host, name, port).await {
            Ok(_) => println!("{}: OK", name),
            Err(e) => println!("{}: FAIL: {:?}", name, e),
        }
    }

    Ok(())
}

async fn execute(path: &str, host: &str, name: &&str, port: &usize) -> Result<String> {
    let start = Instant::now();
    let mut settings = HashMap::new();
    settings.insert(CSV_READER_BATCH_SIZE, "1024");
    let ctx = Context::remote(host, *port, settings);
    let response = ctx
        .read_csv(path, CsvReadOptions::new().schema(&nyctaxi_schema()))?
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

    fn check(x: usize, y: usize, label: &str) -> Result<String> {
        if x == y {
            Ok(format!("{} has correct count", label))
        } else {
            Err(General(format!("{:?} not equal to {:?}", x, y)))
        }
    }

    let mut tests = Vec::new();

    tests.push(check(1, response.len(), "Length"));
    let batch = &response[0];
    tests.push(check(3, batch.num_columns(), "Number of Columns"));
    tests.push(check(10, batch.num_rows(), "Number of Rows"));

    println!("{:?}", &response[0].schema());

    let passenger_count = batch.column(0);
    let min_fare_amt = batch.column(1);
    let max_fare_amt = batch.column(2);

    fn check_type(x: &DataType, y: DataType, label: &str) -> Result<String> {
        if x == &y {
            Ok(format!("Column {} has correct data type", label))
        } else {
            Err(General(format!("{:?} not equal to {:?}", x, y)))
        }
    }

    tests.push(check_type(
        passenger_count.data_type(),
        DataType::Int32,
        "passenger_count",
    ));
    tests.push(check_type(
        min_fare_amt.data_type(),
        DataType::Float64,
        "min_fare_amt",
    ));
    tests.push(check_type(
        max_fare_amt.data_type(),
        DataType::Float64,
        "max_fare_amt",
    ));

    tests.into_iter().collect()
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
