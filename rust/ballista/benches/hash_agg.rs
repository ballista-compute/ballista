use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::datafusion::logicalplan::ScalarValue;
use ballista::execution::expressions::{col, sum};
use ballista::execution::physical_plan::{AggregateMode, ColumnarValue};
use ballista::utils::datagen::DataGen;

use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut gen = DataGen::default();

    let schema = Schema::new(vec![
        Field::new("c0", DataType::Int8, true),
        Field::new("c1", DataType::Int32, false),
    ]);
    let batch = gen.create_batch(&schema, 1024).unwrap();
    let array = batch.column(0);

    let aggr_expr = sum(col(1, "c1"));
    let mut accum = aggr_expr.create_accumulator(&AggregateMode::Partial);

    c.bench_function("sum accum array", |b| b.iter(|| accum.accumulate(&array)));

    c.bench_function("sum accum scalar none", |b| {
        b.iter(|| accum.accumulate(&ColumnarValue::Scalar(Some(ScalarValue::Float64(0_f64)), 1)))
    });

    c.bench_function("sum accum scalar some", |b| {
        b.iter(|| accum.accumulate(&ColumnarValue::Scalar(None, 1)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
