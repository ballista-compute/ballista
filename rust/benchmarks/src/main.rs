use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::process;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

extern crate ballista;

use ballista::cluster;
use ballista::dataframe::{max, min, sum, Context, DataFrame, CSV_BATCH_SIZE};
use ballista::error::{BallistaError, Result};
use ballista::logicalplan::*;
use ballista::BALLISTA_VERSION;

use datafusion::utils;

use std::collections::HashMap;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Ballista Rust Benchmarks v{}", BALLISTA_VERSION);

    let mode = env::var("BENCH_MODE").unwrap();
    let path = env::var("BENCH_PATH").unwrap();
    let format = env::var("BENCH_FORMAT").unwrap();
    let result_filename = env::var("BENCH_RESULT_FILE").unwrap();

    match mode.as_str() {
        "local" => local_mode_benchmark(&path, &result_filename, &format).await?,
        "k8s" => k8s(&path, &format).await?,
        _ => {
            println!("Invalid mode {}", mode);
        }
    }

    Ok(())
}

async fn local_mode_benchmark(path: &str, results_filename: &str, format: &str) -> Result<()> {
    let start = Instant::now();

    let mut settings = HashMap::new();
    settings.insert(CSV_BATCH_SIZE, "4096");

    let ctx = Context::local(settings);
    let df = create_query(&ctx, path, format)?;
    df.explain();

    let response = df.collect().await?;
    utils::print_batches(&response).map_err(|e| BallistaError::DataFusionError(e))?;
    let duration = start.elapsed().as_millis();
    println!("Local mode benchmark took {} ms", duration);

    let mut file = File::create(results_filename).unwrap();
    file.write_all(b"iterations,time_millis\n").unwrap();
    file.write_all(format!("1,{}\n", duration).as_bytes())
        .unwrap();

    Ok(())
}

async fn k8s(path: &str, format: &str) -> Result<()> {
    let cluster_name = "ballista";
    let namespace = "default";
    let num_months: usize = 12;

    // get a list of ballista executors from kubernetes
    let executors = cluster::get_executors(cluster_name, namespace)?;

    if executors.is_empty() {
        println!("No executors found");
        process::exit(1);
    }
    println!("Found {} executors", executors.len());

    let start = Instant::now();

    // execute aggregate query in parallel across all files
    let mut batches: Vec<RecordBatch> = vec![];
    let mut tasks: Vec<task::JoinHandle<Result<Vec<RecordBatch>>>> = vec![];
    let mut executor_index = 0;
    for month in 0..num_months {
        // round robin across the executors
        let executor = &executors[executor_index];
        executor_index += 1;
        if executor_index == executors.len() {
            executor_index = 0;
        }

        let host = executor.host.clone();
        let port = executor.port;
        let path = path.to_owned();
        let format = format.to_owned();

        // execute the query against the executor
        tasks.push(tokio::spawn(async move {
            let filename = format!("{}/yellow_tripdata_2019-{:02}.csv", path, month + 1);

            execute_remote(&host, port, &filename, &format).await
        }));
    }

    // collect the results
    for handle in tasks {
        match handle.await {
            Ok(results) => {
                for batch in results? {
                    batches.push(batch);
                }
            }
            Err(e) => {
                println!("Thread panicked: {:?}", e);
                process::exit(2);
            }
        }
    }

    if batches.is_empty() {
        println!("No data returned from executors!");
        process::exit(3);
    }
    println!("Received {} batches from executors", batches.len());
    utils::print_batches(&batches)?;

    // perform secondary aggregate query on the results collected from the executors
    let mut settings = HashMap::new();
    settings.insert(CSV_BATCH_SIZE, "1024");

    let ctx = Context::local(settings);

    let results = ctx
        .create_dataframe(&batches)?
        .aggregate(
            vec![col("passenger_count")],
            vec![min(col("MIN")), max(col("MAX")), sum(col("SUM"))],
        )?
        .collect()
        .await?;

    // print the results
    println!("Benchmark took {} ms", start.elapsed().as_millis());
    utils::print_batches(&results)?;

    Ok(())
}

/// Execute a query against a remote executor
async fn execute_remote(
    host: &str,
    port: usize,
    filename: &str,
    format: &str,
) -> Result<Vec<RecordBatch>> {
    println!("Executing query against executor at {}:{}", host, port);
    let start = Instant::now();
    let mut settings = HashMap::new();
    settings.insert(CSV_BATCH_SIZE, "1024");
    let ctx = Context::remote(host, port, settings);
    let df = create_query(&ctx, filename, format)?;
    let response = df.collect().await?;
    println!(
        "Executed query against executor at {}:{} in {} seconds",
        host,
        port,
        start.elapsed().as_secs()
    );
    Ok(response)
}

fn create_query(ctx: &Context, path: &str, format: &str) -> Result<DataFrame> {
    let data = match format {
        "csv" => ctx.read_csv(path, Some(nyctaxi_schema()), None, true),
        "parquet" => ctx.read_parquet(path, None),
        _ => Err(BallistaError::General(format!(
            "Unsupported file format '{}'",
            format
        ))),
    }?;

    data.aggregate(
        vec![col("passenger_count")],
        //TODO use aliases for aggregates
        vec![
            min(col("fare_amount")),
            max(col("fare_amount")),
            sum(col("fare_amount")),
        ],
    )
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
