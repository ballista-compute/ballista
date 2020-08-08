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

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::dataframe::{avg, count, max, min, sum};
use ballista::datafusion::logicalplan::col_index;
use ballista::distributed::context::BallistaContext;
use ballista::distributed::executor::{DiscoveryMode, ExecutorConfig};
use ballista::execution::operators::FilterExec;
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{AggregateMode, ColumnarBatchStream, PhysicalPlan};
use ballista::utils::datagen::DataGen;
use std::collections::HashMap;
use std::time::Instant;

async fn execute(use_filter: bool) {
    //TODO remove unwraps

    let mut gen = DataGen::default();

    let schema = Schema::new(vec![
        Field::new("c0", DataType::Int8, true),
        Field::new("c1", DataType::Int32, false),
    ]);
    let batch = gen.create_batch(&schema, 1024).unwrap();

    let mut child = PhysicalPlan::InMemoryTableScan(Arc::new(InMemoryTableScanExec::new(vec![
        batch.clone(),
        batch,
    ])));

    if use_filter {
        // WHERE col(0) >= col(0), which must not affect the final result
        child = PhysicalPlan::Filter(Arc::new(FilterExec::new(
            &child,
            &col_index(0).gt_eq(&col_index(0)),
        )));
    }

    let hash_agg = PhysicalPlan::HashAggregate(Arc::new(
        HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![col_index(0)],
            vec![
                min(col_index(1)).alias("max_c1"),
                max(col_index(1)),
                avg(col_index(1)),
                sum(col_index(1)),
                count(col_index(1)),
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

    assert_eq!(251, batch.num_rows());
    assert_eq!(6, batch.num_columns());

    assert_eq!(batch.column(0).data_type(), &DataType::Int8);
    assert_eq!(batch.column(1).data_type(), &DataType::Int64);
    assert_eq!(batch.column(2).data_type(), &DataType::Int64);
    assert_eq!(batch.column(3).data_type(), &DataType::Float64);
    assert_eq!(batch.column(4).data_type(), &DataType::Int64);
    assert_eq!(batch.column(5).data_type(), &DataType::UInt64);
}

#[test]
fn hash_aggregate() -> std::io::Result<()> {
    smol::run(async {
        execute(false).await;
        std::io::Result::Ok(())
    })
}

#[test]
fn hash_aggregate_with_filter() -> std::io::Result<()> {
    smol::run(async {
        execute(true).await;
        std::io::Result::Ok(())
    })
}
