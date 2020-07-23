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
use ballista::arrow::record_batch::RecordBatch;
use ballista::arrow::util::pretty;
use ballista::dataframe::*;
use ballista::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    //TODO use command-line args
    let path = "/mnt/tpch/parquet/10-24/lineitem";
    let executor_host = "localhost";
    let executor_port = 50051;
    let query_no = 1;

    let start = Instant::now();
    let ctx = Context::remote(executor_host, executor_port, HashMap::new());

    let results = match query_no {
        1 => q1(&ctx, path).await?,
        _ => unimplemented!(),
    };

    // print the results
    pretty::print_batches(&results)?;

    println!("Distributed query took {} ms", start.elapsed().as_millis());

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
async fn q1(ctx: &Context, path: &str) -> Result<Vec<RecordBatch>> {
    // TODO this is WIP and not the real query yet

    // ctx.read_csv(path, options, None)?
    let df = ctx
        .read_parquet(path, None)?
        // .filter(col("l_shipdate").lt(&lit_str("1998-09-01")))? // should be l_shipdate <= date '1998-12-01' - interval ':1' day (3)
        .aggregate(
            vec![col("l_returnflag"), col("l_linestatus")],
            vec![
                sum(col("l_quantity")).alias("sum_qty"),
                // sum(col("l_extendedprice")).alias("sum_base_price"),
                // sum(mult(
                //     &col("l_extendedprice"),
                //     &subtract(&lit_f64(1_f64), &col("l_discount")),
                // ))
                // .alias("sum_disc_price"),
                // sum(mult(
                //     &mult(
                //         &col("l_extendedprice"),
                //         &subtract(&lit_f64(1_f64), &col("l_discount")),
                //     ),
                //     &add(&lit_f64(1_f64), &col("l_tax")),
                // ))
                // .alias("sum_charge"),
                // avg(col("l_quantity")).alias("avg_qty"),
                // avg(col("l_extendedprice")).alias("avg_price"),
                // avg(col("l_discount")).alias("avg_disc"),
                // count(col("l_quantity")).alias("count_order"), // should be count(*) not count(col)
            ],
        )?;
    //.sort()?

    df.explain();

    df.collect().await
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
