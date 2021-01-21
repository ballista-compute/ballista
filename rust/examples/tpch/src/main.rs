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
use std::process::exit;
use std::time::Instant;

extern crate arrow;
extern crate ballista;
extern crate datafusion;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty;
use ballista::prelude::*;
use datafusion::prelude::*;

use ballista::context::BallistaDataFrame;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "tpch")]
struct Opt {
    #[structopt()]
    format: String,

    #[structopt()]
    path: String,

    #[structopt()]
    query: usize,

    #[structopt(short = "h", long = "host", default_value = "localhost")]
    executor_host: String,

    #[structopt(short = "p", long = "port", default_value = "50051")]
    executor_port: usize,
}

enum FileFormat {
    Csv,
    Parquet,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    let format = opt.format.as_str();
    let path = opt.path.as_str();
    let query_no = opt.query;
    let executor_host = opt.executor_host.as_str();
    let executor_port = opt.executor_port;

    let format = match format {
        "csv" => FileFormat::Csv,
        "parquet" => FileFormat::Parquet,
        _ => {
            println!("Invalid file format");
            exit(-1);
        }
    };

    let start = Instant::now();

    let mut settings = HashMap::new();
    settings.insert("parquet.reader.batch.size", "65536");

    // TODO enable these when we get Spark demo working again
    // spark_settings.insert("spark.app.name", "rust-client-demo");
    // spark_settings.insert("spark.ballista.host", "localhost");
    // spark_settings.insert("spark.ballista.port", "50051");
    // spark_settings.insert("spark.executor.memory", "4g");
    // spark_settings.insert("spark.executor.cores", "4");

    let ctx = BallistaContext::remote(executor_host, executor_port, settings);

    let df = match query_no {
        1 => q1(&ctx, path, &format).await?,
        6 => q6(&ctx, path, &format).await?,
        _ => {
            println!("Invalid query no");
            exit(-1);
        }
    };

    let mut stream = df.collect().await?;
    let mut batches = vec![];
    while let Some(result) = stream.next().await {
        let batch = result?;
        batches.push(batch);
    }

    // print the results
    pretty::print_batches(&batches)?;

    println!(
        "TPC-H query {} took {} ms",
        query_no,
        start.elapsed().as_millis()
    );

    Ok(())
}

/// TPCH Query 1.
///
/// The full SQL is:
///
/// select
///     l_returnflag,
///     l_linestatus,
///     sum(l_quantity) as sum_qty,
///     sum(l_extendedprice) as sum_base_price,
///     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
///     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
///     avg(l_quantity) as avg_qty,
///     avg(l_extendedprice) as avg_price,
///     avg(l_discount) as avg_disc,
///     count(*) as count_order
/// from
///     lineitem
/// where
///     l_shipdate <= date '1998-12-01' - interval ':1' day (3)
/// group by
///     l_returnflag,
///     l_linestatus
/// order by
///     l_returnflag,
///     l_linestatus;
///
async fn q1(ctx: &BallistaContext, path: &str, format: &FileFormat) -> Result<BallistaDataFrame> {
    // TODO this is WIP and not the real query yet

    let input = match format {
        FileFormat::Csv => {
            let schema = lineitem_schema();
            let options = CsvReadOptions::new().schema(&schema).has_header(true);
            ctx.read_csv(path, options)?
        }
        FileFormat::Parquet => ctx.read_parquet(path)?,
    };

    input
        .filter(col("l_shipdate").lt(lit("1998-09-01")))? // should be l_shipdate <= date '1998-12-01' - interval ':1' day (3)
        // .project(vec![
        //     col("l_returnflag"),
        //     col("l_linestatus"),
        //     col("l_quantity"),
        //     col("l_extendedprice"),
        //     col("l_tax"),
        //     col("l_discount"),
        //     mult(
        //         &col("l_extendedprice"),
        //         &subtract(&lit_f64(1_f64), &col("l_discount")),
        //     )
        //     .alias("disc_price"),
        // ])?
        .aggregate(
            vec![col("l_returnflag"), col("l_linestatus")],
            vec![
                sum(col("l_quantity")).alias("sum_qty"),
                sum(col("l_extendedprice")).alias("sum_base_price"),
                sum(col("l_extendedprice") * lit(1_f64) - col("l_discount"))
                    .alias("sum_disc_price"),
                sum((col("l_extendedprice") * (lit(1_f64) - col("l_discount")))
                    * (lit(1_f64) + col("l_tax")))
                .alias("sum_charge"),
                avg(col("l_quantity")).alias("avg_qty"),
                avg(col("l_extendedprice")).alias("avg_price"),
                avg(col("l_discount")).alias("avg_disc"),
                count(col("l_quantity")).alias("count_order"), // should be count(*) not count(col)
            ],
        )
}

/// TPCH Query 6.
///
/// The Full SQL is:
///
/// select
///     sum(l_extendedprice * l_discount) as revenue
/// from
///     lineitem
/// where
///     l_shipdate >= date ':1'
///     and l_shipdate < date ':1' + interval '1' year
///     and l_discount between :2 - 0.01 and :2 + 0.01
///     and l_quantity < :3;
///
async fn q6(ctx: &BallistaContext, path: &str, format: &FileFormat) -> Result<BallistaDataFrame> {
    let input = match format {
        FileFormat::Csv => {
            let schema = lineitem_schema();
            let options = CsvReadOptions::new().schema(&schema).has_header(true);
            ctx.read_csv(path, options)?
        }
        FileFormat::Parquet => ctx.read_parquet(path)?,
    };

    input
        .filter(col("l_shipdate").gt_eq(lit("1994-01-01")))?
        .filter(col("l_shipdate").lt(lit("1995-01-01")))?
        .filter(col("l_discount").gt_eq(lit(0.05)))?
        .filter(col("l_discount").lt_eq(lit(0.07)))?
        .filter(col("l_quantity").lt(lit(24.0)))?
        .select(vec![
            (col("l_extendedprice") * col("l_discount")).alias("disc_price")
        ])?
        .aggregate(vec![], vec![sum(col("disc_price")).alias("revenue")])
}

#[allow(dead_code)]
fn lineitem_schema() -> Schema {
    Schema::new(vec![
        Field::new("l_orderkey", DataType::UInt32, true),
        Field::new("l_partkey", DataType::UInt32, true),
        Field::new("l_suppkey", DataType::UInt32, true),
        Field::new("l_linenumber", DataType::UInt32, true),
        Field::new("l_quantity", DataType::Float64, true),
        Field::new("l_extendedprice", DataType::Float64, true),
        Field::new("l_discount", DataType::Float64, true),
        Field::new("l_tax", DataType::Float64, true),
        Field::new("l_returnflag", DataType::Utf8, true),
        Field::new("l_linestatus", DataType::Utf8, true),
        Field::new("l_shipdate", DataType::Utf8, true),
        Field::new("l_commitdate", DataType::Utf8, true),
        Field::new("l_receiptdate", DataType::Utf8, true),
        Field::new("l_shipinstruct", DataType::Utf8, true),
        Field::new("l_shipmode", DataType::Utf8, true),
        Field::new("l_comment", DataType::Utf8, true),
    ])
}
