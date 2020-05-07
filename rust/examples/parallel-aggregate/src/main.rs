use std::process;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

extern crate ballista;

use ballista::cluster;
use ballista::cluster::Executor;
use ballista::dataframe::{max, Context};
use ballista::error::Result;
use ballista::logicalplan::*;
use ballista::BALLISTA_VERSION;

use datafusion::utils;

use std::collections::HashMap;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    println!(
        "Ballista v{} Parallel Aggregate Query Example",
        BALLISTA_VERSION
    );

    //TODO use command-line args
    let nyc_taxi_path = "/mnt/nyctaxi";
    let cluster_name = "ballista";
    let namespace = "default";
    let num_months: usize = 1;

    // for switching between local mode and k8s
    let mode = "local";
    // let mode = "k8s";

    // get a list of ballista executors from kubernetes
    let executors = match mode {
        "local" => vec![Executor::new("localhost", 50051)],
        "k8s" => cluster::get_executors(cluster_name, namespace)?,
        _ => panic!("Invalid mode"),
    };

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

        // execute the query against the executor
        tasks.push(tokio::spawn(async move {
            let filename = format!(
                "{}/csv/yellow/2019/yellow_tripdata_2019-{:02}.csv",
                nyc_taxi_path,
                month + 1
            );

            execute_remote(&host, port, &filename).await
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

    // perform secondary aggregate query on the results collected from the executors
    let ctx = Context::local(HashMap::new());

    let results = ctx
        .create_dataframe(&batches)?
        .aggregate(vec![col("passenger_count")], vec![max(col("fare_amount"))])?
        .collect()
        .await?;

    // print the results
    utils::print_batches(&results)?;

    println!("Parallel query took {} seconds", start.elapsed().as_secs());

    Ok(())
}

/// Execute a query against a remote executor
async fn execute_remote(host: &str, port: usize, filename: &str) -> Result<Vec<RecordBatch>> {
    println!("Executing query against executor at {}:{}", host, port);
    let start = Instant::now();

    let ctx = Context::remote(host, port, HashMap::new());

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

    Ok(response)
}

fn nyctaxi_schema() -> Schema {
    Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::UInt32, true),
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
