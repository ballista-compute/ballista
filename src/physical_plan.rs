//! Ballista physical query plan

use arrow::datatypes::Schema;

pub trait PhysicalPlan {}

/// Projection filters the input by column
#[allow(dead_code)]
pub struct Projection {
    columns: Vec<usize>,
    input: Box<dyn PhysicalPlan>,
}

impl PhysicalPlan for Projection {}

/// Selection filters the input by row
#[allow(dead_code)]
pub struct Selection {
    filter: Box<dyn PhysicalExpr>,
    input: Box<dyn PhysicalPlan>,
}

/// GroupAggregate assumes inputs are ordered by the grouping expression and merges the
/// results without having to maintain a hash map
#[allow(dead_code)]
pub struct GroupAggregate {
    group_expr: Vec<Box<dyn PhysicalExpr>>,
    aggr_expr: Vec<Box<dyn PhysicalExpr>>,
    input: Box<dyn PhysicalPlan>,
}

/// HashAggregate assumes that the input is unordered and uses a hash map to maintain
/// accumulators per grouping hash
#[allow(dead_code)]
pub struct HashAggregate {
    group_expr: Vec<Box<dyn PhysicalExpr>>,
    aggr_expr: Vec<Box<dyn PhysicalExpr>>,
    input: Box<dyn PhysicalPlan>,
}

impl HashAggregate {
    pub fn new(
        group_expr: Vec<Box<dyn PhysicalExpr>>,
        aggr_expr: Vec<Box<dyn PhysicalExpr>>,
        input: Box<dyn PhysicalPlan>,
    ) -> Self {
        Self {
            group_expr,
            aggr_expr,
            input,
        }
    }
}

impl PhysicalPlan for HashAggregate {}

/// Merge to a single partition
#[allow(dead_code)]
pub struct Merge {
    input: Box<dyn PhysicalPlan>,
}

impl Merge {
    pub fn new(input: Box<dyn PhysicalPlan>) -> Self {
        Self { input }
    }
}

impl PhysicalPlan for Merge {}

/// Represents a partitioned file scan
#[allow(dead_code)]
pub struct FileScan {
    path: Vec<String>,
    schema: Box<Schema>,
    projection: Vec<usize>,
}

impl FileScan {
    pub fn new(path: Vec<String>, schema: Box<Schema>, projection: Vec<usize>) -> Self {
        FileScan {
            path,
            schema,
            projection,
        }
    }
}

impl PhysicalPlan for FileScan {}

pub trait PhysicalExpr {}

/// Column reference
#[allow(dead_code)]
pub struct Column {
    index: usize,
}

impl Column {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl PhysicalExpr for Column {}

pub fn col(index: usize) -> Box<Column> {
    Box::new(Column::new(index))
}

/// MAX aggregate function
#[allow(dead_code)]
pub struct Max {
    expr: Box<dyn PhysicalExpr>,
}

impl Max {
    pub fn new(expr: Box<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for Max {}

pub fn max(expr: Box<dyn PhysicalExpr>) -> Box<Max> {
    Box::new(Max::new(expr))
}

/// MIN aggregate function
#[allow(dead_code)]
pub struct Min {
    expr: Box<dyn PhysicalExpr>,
}

impl Min {
    pub fn new(expr: Box<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for Min {}

pub fn min(expr: Box<dyn PhysicalExpr>) -> Box<Min> {
    Box::new(Min::new(expr))
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::physical_plan::*;
    use arrow::datatypes::{DataType, Field, Schema};

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

        let _ = HashAggregate::new(
            vec![col(0)],
            vec![min(col(1)), max(col(1))],
            Box::new(Merge::new(Box::new(HashAggregate::new(
                vec![col(0)],
                vec![min(col(1)), max(col(1))],
                Box::new(FileScan::new(
                    vec!["file1.csv".to_string(), "file2.csv".to_string()],
                    Box::new(schema),
                    vec![3, 10],
                )),
            )))),
        );

        Ok(())
    }
}
