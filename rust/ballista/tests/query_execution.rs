extern crate ballista;

use std::sync::Arc;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::dataframe::{avg, count, max, min, sum};
use ballista::datafusion::logicalplan::col_index;
use ballista::distributed::executor::{DefaultContext, DiscoveryMode, ExecutorConfig};
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{AggregateMode, ColumnarBatchStream, PhysicalPlan};
use ballista::utils::datagen::DataGen;
use std::collections::HashMap;

#[test]
fn hash_aggregate() -> std::io::Result<()> {
    smol::run(async {
        //TODO remove unwraps

        let mut gen = DataGen::default();

        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int8, true),
            Field::new("c1", DataType::Int32, false),
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
                vec![
                    min(col_index(1)),
                    max(col_index(1)),
                    avg(col_index(1)),
                    sum(col_index(1)),
                    count(col_index(1)),
                ],
                Arc::new(in_memory_exec),
            )
            .unwrap(),
        ));

        let config = ExecutorConfig::new(DiscoveryMode::Standalone, "", 0, "");

        let ctx = Arc::new(DefaultContext::new(&config, HashMap::new()));

        let stream: ColumnarBatchStream =
            hash_agg.as_execution_plan().execute(ctx, 0).await.unwrap();
        let mut results = vec![];
        while let Some(batch) = stream.next().await.unwrap() {
            results.push(batch);
        }

        assert_eq!(1, results.len());

        let batch = &results[0];

        assert_eq!(256, batch.num_rows());
        assert_eq!(6, batch.num_columns());

        std::io::Result::Ok(())
    })
}
