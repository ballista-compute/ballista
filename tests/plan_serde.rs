extern crate ballista;

#[macro_use]
extern crate log;

use arrow::datatypes::{DataType, Field, Schema};
use ballista::error::Result;
use ballista::execution;
use ballista::logical_plan;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::LogicalPlan;

use std::sync::Arc;

#[test]
fn test_aggregate_roundtrip() -> Result<()> {
    let _ = ::env_logger::init();

    // schema for nyxtaxi csv files
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Float64, true),
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

    let month = 0;

    let filename = format!(
        "/mnt/ssd/nyc_taxis/csv/yellow_tripdata_2018-{:02}.csv",
        month + 1
    );

    // create DataFusion query plan to execute on each partition
    let mut ctx = ExecutionContext::new();
    ctx.register_csv("tripdata", &filename, &schema, true);
    let logical_plan = ctx.create_logical_plan(
        "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
    )?;
    let logical_plan = ctx.optimize(&logical_plan)?;

    println!("Logical plan: {:?}", logical_plan);

    // execute query just to be sure the plan is valid
    //execute(&mut ctx, &logical_plan)?;

    // convert
    let plan = round_trip(&logical_plan)?;
    println!("Logical plan after serde: {:?}", logical_plan);

    let original_plan_str = format!("{:?}", logical_plan);
    let new_plan_str = format!("{:?}", plan);

    assert_eq!(original_plan_str, new_plan_str);

    // execute query just to be sure the plan is valid
    //execute(&mut ctx, &plan)?;

    Ok(())
}

fn round_trip(plan: &LogicalPlan) -> Result<Arc<LogicalPlan>> {
    // convert to ballista plan
    let ballista_plan = logical_plan::convert_to_ballista_plan(plan)?;
    //println!("ballista: {:?}", ballista_plan);

    // convert back to DataFusion plan
    let table = execution::create_datafusion_plan(&ballista_plan.to_proto())?;

    Ok(table.to_logical_plan())
}

fn execute(ctx: &mut ExecutionContext, logical_plan: &LogicalPlan) -> Result<()> {
    println!("Executing query: {:?}", logical_plan);
    let result = ctx.execute(logical_plan, 1024)?;
    let mut result = result.borrow_mut();
    while let Some(batch) = result.next()? {
        println!(
            "Fetched {} rows x {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    println!("End of results");
    Ok(())
}
