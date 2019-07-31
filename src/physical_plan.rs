//! Ballista physical query plan

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

// NOTE that the ExecutionPlan, Partition, and BatchIterator traits are hopefully moving to Arrow (see https://github.com/apache/arrow/pull/4975)

/// Partition-aware execution plan for a relation
pub trait ExecutionPlan {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema>;
    /// Get the partitions for this execution plan. Each partition can be executed in parallel.
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>>;
}

/// Represents a partition of an execution plan that can be executed on a thread
pub trait Partition: Send + Sync {
    /// Execute this partition and return an iterator over RecordBatch
    fn execute(&self) -> Result<Arc<dyn BatchIterator>>;
}

/// Iterator over RecordBatch that can be sent between threads
pub trait BatchIterator: Send + Sync {
    /// Get the next RecordBatch
    fn next(&self) -> Result<Option<RecordBatch>>;
}

/// Projection filters the input by column
#[allow(dead_code)]
pub struct Projection {
    columns: Vec<usize>,
    input: Box<dyn ExecutionPlan>,
}

impl ExecutionPlan for Projection {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self.input.partitions()?.iter().map(|p| {
            //TODO inject projection logic
            p.clone()
        }).collect())
    }

}

/// Selection filters the input by row
#[allow(dead_code)]
pub struct Selection {
    filter: Box<dyn PhysicalExpr>,
    input: Box<dyn ExecutionPlan>,
}

impl ExecutionPlan for Selection {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self.input.partitions()?.iter().map(|p| {
            //TODO inject selection logic
            p.clone()
        }).collect())
    }

}

/// GroupAggregate assumes inputs are ordered by the grouping expression and merges the
/// results without having to maintain a hash map
#[allow(dead_code)]
pub struct GroupAggregate {
    group_expr: Vec<Box<dyn PhysicalExpr>>,
    aggr_expr: Vec<Box<dyn PhysicalExpr>>,
    input: Box<dyn ExecutionPlan>,
}

/// HashAggregate assumes that the input is unordered and uses a hash map to maintain
/// accumulators per grouping hash
#[allow(dead_code)]
pub struct HashAggregate {
    group_expr: Vec<Box<dyn PhysicalExpr>>,
    aggr_expr: Vec<Box<dyn PhysicalExpr>>,
    input: Box<dyn ExecutionPlan>,
}

impl HashAggregate {
    pub fn new(
        group_expr: Vec<Box<dyn PhysicalExpr>>,
        aggr_expr: Vec<Box<dyn PhysicalExpr>>,
        input: Box<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            group_expr,
            aggr_expr,
            input,
        }
    }
}

impl ExecutionPlan for HashAggregate {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(self.input.partitions()?.iter().map(|p| {
            //TODO inject hash aggregate logic
            p.clone()
        }).collect())
    }

}

/// Merge to a single partition
#[allow(dead_code)]
pub struct Merge {
    input: Box<dyn ExecutionPlan>,
}

impl Merge {
    pub fn new(input: Box<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl ExecutionPlan for Merge {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        //TODO run each partition on a thread and merge the results
        unimplemented!()
    }
}

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

impl ExecutionPlan for FileScan {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        unimplemented!()
    }

}

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
