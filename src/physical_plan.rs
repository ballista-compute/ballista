//! Ballista physical query plan

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::table_impl::TableImpl;
use datafusion::logicalplan::Expr;
use std::sync::Arc;

/// Ballista physical query plan. Note that most of the variants here are just placeholders
/// currently. Also note that this plan references Expr from the DataFusion logical plan
/// and this may change in the future.
#[allow(dead_code)]
#[derive(Debug, Clone)]
enum PhysicalPlan {
    /// Projection filters the input by column
    Projection {
        columns: Vec<usize>,
        input: Box<PhysicalPlan>,
    },
    /// Selection filters the input by row
    Selection {
        filter: Expr,
        input: Box<PhysicalPlan>,
    },
    /// GroupAggregate assumes inputs are ordered by the grouping expression and merges the
    /// results without having to maintain a hash map
    GroupAggregate {
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    /// HashAggregate assumes that the input is unordered and uses a hash map to maintain
    /// accumulators per grouping hash
    HashAggregate {
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    Sort {
        sort_expr: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    /// Merge the input for each partition without additional processing and combine into a
    /// single partition.
    Merge { input: Box<PhysicalPlan> },
    /// Represents a file scan
    FileScan {
        path: String,
        projection: Vec<usize>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::PhysicalPlan::*;
    use datafusion::logicalplan::LogicalPlan;
    use datafusion::table::Table;

    #[test]
    fn aggregate() -> Result<()> {
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

        // construct the HashAggregate that will run on each executor
        let mut ctx = ExecutionContext::new();
        ctx.register_csv("tripdata", "file.csv", &schema, true);
        let table = ctx.table("tripdata")?;
        let passenger_count = table.col("passenger_count")?;
        let fare_amount = table.col("fare_amount")?;
        let aggr = HashAggregate {
            group_expr: vec![passenger_count],
            aggr_expr: vec![table.min(&fare_amount)?, table.max(&fare_amount)?],
            input: Box::new(FileScan {
                path: "/data".to_string(),
                projection: vec![3, 10],
            }),
        };

        // next step is to merge the results into a single partition
        let merge = Merge {
            input: Box::new(aggr),
        };

        // finally we need to perform a new aggregate
        let merge_schema = Schema::new(vec![
            Field::new("passenger_count", DataType::Utf8, true),
            Field::new("min_fare_amount", DataType::Float64, true),
            Field::new("max_fare_amount", DataType::Float64, true),
        ]);
        let merge_logical_plan = LogicalPlan::TableScan {
            schema_name: "".to_string(),
            table_name: "".to_string(),
            table_schema: Arc::new(merge_schema.clone()),
            projected_schema: Arc::new(merge_schema),
            projection: None,
        };
        let table = TableImpl::new(Arc::new(merge_logical_plan));

        let final_aggr = HashAggregate {
            group_expr: vec![table.col("passenger_count")?],
            aggr_expr: vec![
                table.min(&table.col("min_fare_amount")?)?,
                table.max(&table.col("max_fare_amount")?)?,
            ],
            input: Box::new(merge),
        };

        println!("Plan: {:?}", final_aggr);
        let expected = "HashAggregate { group_expr: [#0], aggr_expr: [MIN(#1), MAX(#2)], input: Merge { input: HashAggregate { group_expr: [#3], aggr_expr: [MIN(#10), MAX(#10)], input: FileScan { path: \"/data\", projection: [3, 10] } } } }";

        assert_eq!(expected, format!("{:?}", final_aggr));

        Ok(())
    }
}
