use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;

use arrow::datatypes::{Schema, Field, DataType};
use arrow::flight::flight_data_to_batch;
use arrow::record_batch::RecordBatch;

use ballista::protobuf;
use ballista::error::BallistaError;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::*;

use prost::bytes::BufMut;
use prost::Message;

//use flight::flight_descriptor;
use flight::flight_service_client::FlightServiceClient;
use flight::Ticket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let schema = Schema::new(vec![
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
    ]);

    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    let mut batches: Vec<RecordBatch> = vec![];

    let num_months: usize = 12;
    for month in 0..num_months {
        let filename = format!(
            "/mnt/data/nyc_taxis/csv/yellow_tripdata_2019-{:02}.csv",
            month + 1
        );

        let plan = LogicalPlanBuilder::scan("default", "tbd", &schema, None)
            .and_then(|plan| plan.aggregate(vec![col(3)], vec![Expr::AggregateFunction {
                name: "MAX".to_owned(), args: vec![col(10)], return_type: DataType::Float64 }]))
            .and_then(|plan| plan.build())
            //.map_err(|e| Err(format!("{:?}", e)))
            .unwrap(); //TODO

        let serialized_plan: protobuf::LogicalPlanNode = plan.try_into().unwrap();
        let mut buf: Vec<u8> = Vec::with_capacity(serialized_plan.encoded_len());
        serialized_plan.encode(&mut buf).unwrap();

        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = client.do_get(request).await?.into_inner();

        // the schema should be the first message returned, else client should error
        let flight_data = stream.message().await?.unwrap();
        // convert FlightData to a stream
        let schema = Arc::new(Schema::try_from(&flight_data)?);
        println!("Schema: {:?}", schema);

        // all the remaining stream messages should be dictionary and record batches
        while let Some(flight_data) = stream.message().await? {
            // the unwrap is infallible and thus safe
            let record_batch = flight_data_to_batch(&flight_data, schema.clone())?.unwrap();
            batches.push(record_batch);
            // println!(
            //     "record_batch has {} columns and {} rows",
            //     record_batch.num_columns(),
            //     record_batch.num_rows()
            // );
            // let column = record_batch.column(0);
            // let column = column
            //     .as_any()
            //     .downcast_ref::<Int32Array>()
            //     .expect("Unable to get column");
            // println!("Column 1: {:?}", column);
        }
    }

    println!("Received {} batches", batches.len());

    // perform secondary aggregate query on the results collected from the executors
    let schema = Schema::new(vec![
        Field::new("passenger_count", DataType::UInt32, true),
        Field::new("fare_amount", DataType::Float64, true),
    ]);

    let plan = LogicalPlanBuilder::scan("default", "tbd", &schema, None)
        .and_then(|plan| plan.aggregate(vec![col(0)], vec![Expr::AggregateFunction {
            name: "MAX".to_owned(), args: vec![col(1)], return_type: DataType::Float64 }]))
        .and_then(|plan| plan.build())
        //.map_err(|e| Err(format!("{:?}", e)))
        .unwrap(); //TODO

    // create local execution context
    let mut ctx = ExecutionContext::new();

    // let testdata =
    //     std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    // register parquet file with the execution context
    // ctx.register_parquet("alltypes_plain", &format!("alltypes_plain.snappy.parquet"))
    //     .map_err(|e| to_tonic_err(&e))?;

    let results = ctx
        .collect_plan(&plan, 1024*1024)
        .unwrap(); // TODO
    //    .map_err(|e| to_tonic_err(&e))?;


    Ok(())
}
