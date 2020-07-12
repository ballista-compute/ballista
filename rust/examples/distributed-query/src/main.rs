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
use ballista::arrow::util::pretty;
use ballista::dataframe::{max, Context};
use ballista::datafusion::logicalplan::*;
use ballista::error::Result;
use ballista::BALLISTA_VERSION;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Ballista v{} Distributed Query Example", BALLISTA_VERSION);

    //TODO use command-line args
    let nyc_taxi_path = "/mnt/nyctaxi/parquet/year=2019";
    let executor_host = "localhost";
    let executor_port = 50051;

    let start = Instant::now();
    let ctx = Context::remote(executor_host, executor_port, HashMap::new());

    let results = ctx
        .read_parquet(nyc_taxi_path, None)?
        .aggregate(vec![col("passenger_count")], vec![max(col("fare_amount"))])?
        .collect()
        .await?;

    // print the results
    pretty::print_batches(&results)?;

    println!(
        "Distributed query took {} seconds",
        start.elapsed().as_secs()
    );

    Ok(())
}
