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

use structopt::StructOpt;

extern crate ballista;
use ballista::arrow::util::pretty;
use ballista::dataframe::{max, Context};
use ballista::datafusion::logicalplan::*;
use ballista::error::Result;
use ballista::BALLISTA_VERSION;

#[derive(StructOpt, Debug)]
#[structopt(name = "example")]
struct Opt {
    #[structopt()]
    path: String,

    #[structopt(short = "h", long = "host", default_value = "localhost")]
    executor_host: String,

    #[structopt(short = "p", long = "port", default_value = "50051")]
    executor_port: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    let path = opt.path.as_str();
    let executor_host = opt.executor_host.as_str();
    let executor_port = opt.executor_port;

    println!("Ballista v{} Distributed Query Example", BALLISTA_VERSION);
    println!("Path: {}", path);
    println!("Host: {}:{}", executor_host, executor_port);

    let start = Instant::now();
    let ctx = Context::remote(executor_host, executor_port, HashMap::new());

    let results = ctx
        .read_parquet(path, None)?
        .aggregate(vec![col("passenger_count")], vec![max(col("fare_amount"))])?
        .collect()
        .await?;

    // print the results
    pretty::print_batches(&results)?;

    println!("Distributed query took {} ms", start.elapsed().as_millis());

    Ok(())
}
