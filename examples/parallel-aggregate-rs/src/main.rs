use std::sync::Arc;
use std::thread;

use arrow::array::Int32Array;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;

use ballista::client;
use ballista::cluster;
use ballista::error::BallistaError;

use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // get a list of ballista executors from kubernetes
    let executors = cluster::get_executors("nyctaxi", "default").unwrap();
    let mut executor_index = 0;

    // execute aggregate query in parallel across all files
    let num_months: usize = 12;
    let threads: Vec<thread::JoinHandle<Result<Vec<RecordBatch>, BallistaError>>> = vec![];
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
        tokio::spawn(async move {
            println!("Executing query against executor at {}:{}", host, port);

            let filename = format!(
                "/mnt/data/nyc_taxis/csv/yellow_tripdata_2019-{:02}.csv",
                month + 1
            );
            let schema = nyctaxi_schema();

            // SELECT passenger_count, MAX(fare_amount) FROM <filename> GROUP BY passenger_count
            let plan = LogicalPlanBuilder::scan("default", &filename, &schema, None)
                .and_then(|plan| plan.aggregate(vec![col(0)], vec![max(col(1))]))
                .and_then(|plan| plan.build())
                //.map_err(|e| Err(format!("{:?}", e)))
                .unwrap(); //TODO

            client::execute_query(&host, port, plan).await
                .map_err(|e| BallistaError::General(format!("{:?}", e)))
        });
    }

    // collect the results
    let mut batches: Vec<RecordBatch> = vec![];
    for handle in threads {
        let response: Vec<RecordBatch> = handle.join().expect("thread panicked").unwrap();
        for batch in response {
            batches.push(batch);
        }
    }
    println!("Received {} batches", batches.len());

    // perform secondary aggregate query on the results collected from the executors
    let mut ctx = ExecutionContext::new();

    let schema = Schema::new(vec![
        Field::new("passenger_count", DataType::UInt32, true),
        Field::new("fare_amount", DataType::Float64, true),
    ]);
    let provider = MemTable::new(Arc::new(schema.clone()), batches).unwrap();
    ctx.register_table("tripdata", Box::new(provider));

    let plan = LogicalPlanBuilder::scan("default", "tripdata", &schema, None)
        .and_then(|plan| plan.aggregate(vec![col(0)], vec![max(col(1))]))
        .and_then(|plan| plan.build())
        //.map_err(|e| Err(format!("{:?}", e)))
        .unwrap(); //TODO

    let results = ctx
        .collect_plan(&plan, 1024*1024)
        .unwrap(); // TODO
    //    .map_err(|e| to_tonic_err(&e))?;

    // print results
    results.iter().for_each(|batch| {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int type");

        let c2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int type");

        for i in 0..batch.num_rows() {
            println!("{}, {}", c1.value(i), c2.value(i),);
        }
    });


    Ok(())
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

fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        name: "MAX".to_owned(),
        args: vec![expr],
        return_type: DataType::Float64
    }
}

