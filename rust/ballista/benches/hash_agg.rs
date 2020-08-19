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

use std::collections::HashMap;
use std::sync::Arc;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::datafusion::logicalplan::{self, Expr};
use ballista::distributed::context::BallistaContext;
use ballista::distributed::executor::{DiscoveryMode, ExecutorConfig};
use ballista::execution::operators::{HashAggregateExec, InMemoryTableScanExec};
use ballista::execution::physical_plan::{AggregateMode, ExecutionPlan, PhysicalPlan};
use ballista::utils::datagen::DataGen;

use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut gen = DataGen::default();
    let schema = Schema::new(vec![
        Field::new("c0", DataType::Int8, true),
        Field::new("c1", DataType::Int32, false),
    ]);
    let batch = gen.create_batch(&schema, 1024).unwrap();
    let config = ExecutorConfig::new(DiscoveryMode::Standalone, "", 0, "", 2);
    let ctx = Arc::new(BallistaContext::new(&config, HashMap::new()));
    let table = Arc::new(PhysicalPlan::InMemoryTableScan(Arc::new(
        InMemoryTableScanExec::new(vec![batch.clone(), batch]),
    )));

    c.bench_function("hash agg sum partial", |b| {
        b.iter(|| {
            let ctx = ctx.clone();
            let table = table.clone();
            smol::run(async move {
                let hash_agg_exec = Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![logicalplan::col("c0")],
                        vec![Expr::AggregateFunction {
                            name: "sum".to_owned(),
                            args: vec![logicalplan::col("c1")],
                        }],
                        table,
                    )
                    .unwrap(),
                );
                let stream = hash_agg_exec.execute(ctx, 0).await.unwrap();
                while let Some(_) = stream.next().unwrap() {}
            })
        })
    });

    c.bench_function("hash agg avg partial", |b| {
        b.iter(|| {
            let ctx = ctx.clone();
            let table = table.clone();
            smol::run(async move {
                let hash_agg_exec = Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![logicalplan::col("c0")],
                        vec![Expr::AggregateFunction {
                            name: "avg".to_owned(),
                            args: vec![logicalplan::col("c1")],
                        }],
                        table,
                    )
                    .unwrap(),
                );
                let stream = hash_agg_exec.execute(ctx, 0).await.unwrap();
                while let Some(_) = stream.next().unwrap() {}
            })
        })
    });

    c.bench_function("hash agg count partial", |b| {
        b.iter(|| {
            let ctx = ctx.clone();
            let table = table.clone();
            smol::run(async move {
                let hash_agg_exec = Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![logicalplan::col("c0")],
                        vec![Expr::AggregateFunction {
                            name: "count".to_owned(),
                            args: vec![logicalplan::col("c1")],
                        }],
                        table,
                    )
                    .unwrap(),
                );
                let stream = hash_agg_exec.execute(ctx, 0).await.unwrap();
                while let Some(_) = stream.next().unwrap() {}
            })
        })
    });

    c.bench_function("hash agg min partial", |b| {
        b.iter(|| {
            let ctx = ctx.clone();
            let table = table.clone();
            smol::run(async move {
                let hash_agg_exec = Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![logicalplan::col("c0")],
                        vec![Expr::AggregateFunction {
                            name: "min".to_owned(),
                            args: vec![logicalplan::col("c1")],
                        }],
                        table,
                    )
                    .unwrap(),
                );
                let stream = hash_agg_exec.execute(ctx, 0).await.unwrap();
                while let Some(_) = stream.next().unwrap() {}
            })
        })
    });

    c.bench_function("hash agg max partial", |b| {
        b.iter(|| {
            let ctx = ctx.clone();
            let table = table.clone();
            smol::run(async move {
                let hash_agg_exec = Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![logicalplan::col("c0")],
                        vec![Expr::AggregateFunction {
                            name: "max".to_owned(),
                            args: vec![logicalplan::col("c1")],
                        }],
                        table,
                    )
                    .unwrap(),
                );
                let stream = hash_agg_exec.execute(ctx, 0).await.unwrap();
                while let Some(_) = stream.next().unwrap() {}
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
