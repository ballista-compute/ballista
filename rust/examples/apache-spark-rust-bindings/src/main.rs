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

extern crate ballista;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::arrow::util::pretty;
use ballista::dataframe::*;
use ballista::datafusion::prelude::*;
use ballista::error::Result;

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
        .read_csv(path, CsvReadOptions::new().schema(&nyctaxi_schema()))?
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
    pretty::print_batches(&results)?;

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
