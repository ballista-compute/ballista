extern crate ballista;

use std::sync::Arc;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::dataframe::max;
use ballista::datafusion::logicalplan::col_index;
use ballista::distributed::executor::ExecutorContext;
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{AggregateMode, ColumnarBatchStream, PhysicalPlan};
use ballista::utils::datagen::DataGen;

#[test]
fn hash_aggregate() -> std::io::Result<()> {
    smol::run(async {
        //TODO remove unwraps

        let mut gen = DataGen::default();

        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, false),
        ]);
        let batch = gen.create_batch(&schema, 4096).unwrap();

        let in_memory_exec =
            PhysicalPlan::InMemoryTableScan(Arc::new(InMemoryTableScanExec::new(vec![
                batch.clone(),
                batch,
            ])));

        let hash_agg = PhysicalPlan::HashAggregate(Arc::new(
            HashAggregateExec::try_new(
                AggregateMode::Partial,
                vec![col_index(0)],
                vec![max(col_index(1))],
                Arc::new(in_memory_exec),
            )
            .unwrap(),
        ));

        let ctx = Arc::new(ExecutorContext {});

        let stream: ColumnarBatchStream =
            hash_agg.as_execution_plan().execute(ctx, 0).await.unwrap();
        let mut results = vec![];
        while let Some(batch) = stream.next().await.unwrap() {
            results.push(batch);
        }

        assert_eq!(1, results.len());

        let batch = &results[0];

        assert_eq!(3961, batch.num_rows());
        assert_eq!(2, batch.num_columns());

        std::io::Result::Ok(())
    })
}
