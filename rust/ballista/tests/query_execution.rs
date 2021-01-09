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

extern crate ballista;

use std::sync::Arc;

use arrow::{array::Int32Array, record_batch::RecordBatch};
use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::datafusion::prelude::*;
use ballista::distributed::context::BallistaContext;
use ballista::distributed::executor::{DiscoveryMode, ExecutorConfig};
use ballista::error::Result;
use ballista::execution::operators::FilterExec;
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{
    AggregateMode, ColumnarBatch, ColumnarBatchStream, PhysicalPlan,
};
use ballista::utils::pretty::result_str;
use std::collections::HashMap;
use std::time::Instant;

pub fn build_table_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Result<(RecordBatch, Schema)> {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )?;
    Ok((batch, schema))
}

async fn execute(use_filter: bool) -> Result<()> {
    let (batch, _) = build_table_i32(
        ("c0", &vec![1, 1, 3]),
        ("c1", &vec![1, 2, 3]),
        ("c2", &vec![1, 2, 3]),
    )?;

    let batch = ColumnarBatch::from_arrow(&batch);

    let batches = vec![batch.clone(), batch];

    let mut child = PhysicalPlan::InMemoryTableScan(Arc::new(InMemoryTableScanExec::new(batches)));

    if use_filter {
        // WHERE col(c0) >= col(c0), which must not affect the final result
        child = PhysicalPlan::Filter(Arc::new(FilterExec::new(
            &child,
            &col("c0").gt_eq(col("c0")),
        )));
    }

    let hash_agg = PhysicalPlan::HashAggregate(Arc::new(
        HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![col("c0")],
            vec![
                min(col("c1")).alias("max_c1"),
                max(col("c1")),
                avg(col("c1")),
                sum(col("c1")),
                count(col("c1")),
            ],
            Arc::new(child),
        )
        .unwrap(),
    ));

    let config = ExecutorConfig::new(DiscoveryMode::Standalone, "", 0, "", 2);

    let ctx = Arc::new(BallistaContext::new(&config, HashMap::new()));

    let start = Instant::now();
    let stream: ColumnarBatchStream = hash_agg.as_execution_plan().execute(ctx, 0).await.unwrap();
    let mut results = vec![];
    while let Some(batch) = stream.next().unwrap() {
        results.push(batch);
    }

    let duration = start.elapsed();
    println!("Took {} ms", duration.as_millis());

    assert_eq!(1, results.len());

    let batch = &results[0];

    assert_eq!(2, batch.num_rows());
    assert_eq!(6, batch.num_columns());

    assert_eq!(batch.column("c0")?.data_type(), &DataType::Int32);
    assert_eq!(
        batch.column("max_c1")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        batch.column("MAX(c1)")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        batch.column("AVG(c1)")?.data_type(),
        &DataType::Float64
    );
    assert_eq!(
        batch.column("SUM(c1)")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        batch.column("COUNT(c1)")?.data_type(),
        &DataType::UInt64
    );

    let result = batch.to_arrow()?;

    let mut r = result_str(&[result])?;

    // there are two batches => sum and count double
    let mut expected = vec!["1\t1\t2\t1.5\t6\t4", "3\t3\t3\t3.0\t6\t2"];

    r.sort();
    expected.sort();
    assert_eq!(r, expected);
    Ok(())
}


#[test]
fn hash_aggregate() -> Result<()> {
    smol::run(async {
        execute(false).await?;
        Ok(())
    })
}

#[test]
fn hash_aggregate_with_filter() -> Result<()> {
    smol::run(async {
        execute(true).await?;
        Ok(())
    })
}
