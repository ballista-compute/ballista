// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Traits for physical query plan, supporting parallel execution for partitioned relations.

use std::sync::{Arc, Mutex};

use crate::error::{BallistaError, Result};
use crate::proto;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::execution::table_impl::TableImpl;
use datafusion::logicalplan::{Expr, LogicalPlan as DFPlan};
use datafusion::table::Table;

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
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>>;
}

/// Iterator over RecordBatch that can be sent between threads
pub trait BatchIterator: Send + Sync {
    /// Get the next RecordBatch
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

/// Expression that can be evaluated against a RecordBatch
pub trait PhysicalExpr: Send + Sync {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

pub mod csv;
pub mod expressions;
pub mod projection;

/// Convert a Ballista protobuf data type to an Arrow DataType
fn to_arrow_type(proto_type: i32) -> Result<DataType> {
    match proto_type {
        1 => Ok(DataType::Boolean),
        2 => Ok(DataType::UInt8),
        3 => Ok(DataType::Int8),
        4 => Ok(DataType::UInt16),
        5 => Ok(DataType::Int16),
        6 => Ok(DataType::UInt32),
        7 => Ok(DataType::Int32),
        8 => Ok(DataType::UInt64),
        9 => Ok(DataType::Int64),
        11 => Ok(DataType::Float32),
        12 => Ok(DataType::Float64),
        13 => Ok(DataType::Utf8),
        _ => Err(BallistaError::General(format!(
            "No conversion for data type {}",
            proto_type
        ))),
        //TODO add others
        // 0 => NONE
        //        HALF_FLOAT = 10;
        //        BINARY = 14;
        //        FIXED_SIZE_BINARY = 15;
        //        DATE32 = 16;
        //        DATE64 = 17;
        //        TIMESTAMP = 18;
        //        TIME32 = 19;
        //        TIME64 = 20;
        //        INTERVAL = 21;
        //        DECIMAL = 22;
        //        LIST = 23;
        //        STRUCT = 24;
        //        UNION = 25;
        //        DICTIONARY = 26;
        //        MAP = 27;
    }
}

/// Convert Ballista schema to Arrow Schema
pub fn create_arrow_schema(schema: &proto::Schema) -> Result<Schema> {
    Ok(Schema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name.to_string(),
                    to_arrow_type(column.arrow_type).unwrap(),
                    true,
                )
            })
            .collect(),
    ))
}

/// Convert Ballista logical plan to DataFusion logical plan
pub fn create_datafusion_plan(plan: &proto::LogicalPlanNode) -> Result<Arc<dyn Table>> {
    if plan.file.is_some() {
        let file = plan.file.as_ref().unwrap();

        let schema = create_arrow_schema(file.schema.as_ref().unwrap())?;

        let projection: Vec<usize> = file.projection.iter().map(|i| *i as usize).collect();

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        println!(
            "created table scan with schema {:?} and projection {:?}",
            schema, projection
        );

        Ok(Arc::new(TableImpl::new(Arc::new(DFPlan::TableScan {
            schema_name: "default".to_string(),
            table_name: file.filename.clone(),
            table_schema: Arc::new(schema.clone()),
            projected_schema: Arc::new(projected_schema),
            projection: Some(projection),
        }))))
    } else if plan.projection.is_some() {
        if let Some(input) = &plan.input {
            let table = create_datafusion_plan(&input)?;

            let projection_plan = plan.projection.as_ref().unwrap();

            let expr: Vec<Expr> = projection_plan
                .expr
                .iter()
                .map(|expr| map_expr(table.as_ref(), expr))
                .collect::<Result<Vec<Expr>>>()?;

            println!("projection: {:?}", expr);

            Ok(table.select(expr)?)
        } else {
            Err(BallistaError::General(
                "Projection with no input is not supported".to_string(),
            ))
        }
    } else if plan.aggregate.is_some() {
        if let Some(input) = &plan.input {
            let table = create_datafusion_plan(&input)?;

            let aggregate_plan = plan.aggregate.as_ref().unwrap();

            let group_expr: Vec<Expr> = aggregate_plan
                .group_expr
                .iter()
                .map(|expr| map_expr(table.as_ref(), expr))
                .collect::<Result<Vec<Expr>>>()?;

            let aggr_expr: Vec<Expr> = aggregate_plan
                .aggr_expr
                .iter()
                .map(|expr| map_expr(table.as_ref(), expr))
                .collect::<Result<Vec<Expr>>>()?;

            Ok(table.aggregate(group_expr, aggr_expr)?)
        } else {
            Err(BallistaError::General(
                "Aggregate with no input is not supported".to_string(),
            ))
        }
    } else {
        Err(BallistaError::General("unsupported plan".to_string()))
    }
}

/// Map Ballista expression to DataFusion expression
fn map_expr(table: &dyn Table, expr: &proto::ExprNode) -> Result<Expr> {
    if expr.column_index.is_some() {
        //TODO delegate to Table API to do this
        Ok(Expr::Column(
            expr.column_index.as_ref().unwrap().index as usize,
        ))
    } else if expr.aggregate_expr.is_some() {
        //TODO cleanup and add error handling
        let aggr_expr = expr.aggregate_expr.as_ref().unwrap();
        let x = aggr_expr.expr.clone().unwrap();

        let input_expr = map_expr(table, &x)?;
        match aggr_expr.aggr_function {
            0 => Ok(table.min(&input_expr)?),
            1 => Ok(table.max(&input_expr)?),
            2 => Ok(table.sum(&input_expr)?),
            3 => Ok(table.avg(&input_expr)?),
            4 => Ok(table.count(&input_expr)?),
            //5 => Ok(table.count_distinct(&input_expr)?),
            _ => Err(BallistaError::General(
                "unsupported aggregate function".to_string(),
            )),
        }
    } else {
        Err(BallistaError::General("unsupported expr".to_string()))
    }
}
