extern crate ballista;

use std::sync::Arc;

use ballista::arrow::datatypes::{DataType, Field, Schema};
use ballista::dataframe::max;
use ballista::datafusion::logicalplan::col_index;
use ballista::datagen::DataGen;
use ballista::distributed::executor::{DefaultExecutor, Executor};
use ballista::distributed::scheduler::{create_job, ensure_requirements, execute_job, LocalModeContext};
use ballista::error::Result;
use ballista::execution::operators::HashAggregateExec;
use ballista::execution::operators::InMemoryTableScanExec;
use ballista::execution::physical_plan::{AggregateMode, PhysicalPlan, ExecutionContext};

#[test]
fn hash_aggregate() -> Result<()> {
    smol::run(async {
        let mut gen = DataGen::default();

        // create in-memory data source with 2 partitions
        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, false),
        ]);
        let batch = gen.create_batch(&schema, 4096)?;
        let partitions = vec![vec![batch.clone()], vec![batch]];
        let in_memory_exec =
            PhysicalPlan::InMemoryTableScan(Arc::new(InMemoryTableScanExec::new(partitions)));

        // partial agg
        let partial_agg = PhysicalPlan::HashAggregate(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![col_index(0)],
            vec![max(col_index(1))],
            Arc::new(in_memory_exec),
        )?));
        println!("partial_agg: {:?}", partial_agg);

        // final agg
        let final_agg = PhysicalPlan::HashAggregate(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            vec![col_index(0)],
            vec![max(col_index(1))],
            Arc::new(partial_agg),
        )?));

        println!("final_agg: {:?}", final_agg);

        // insert shuffle
        let plan = ensure_requirements(&final_agg)?;
        println!("plan after ensure_requirements: {:?}", plan);

        let expected = "HashAggregate: mode=Final, groupExpr=[#0], aggrExpr=[MAX(#1)]
  Shuffle: UnknownPartitioning(0)
    HashAggregate: mode=Partial, groupExpr=[#0], aggrExpr=[MAX(#1)]
      InMemoryTableScan:";

        assert_eq!(expected, format!("{:?}", plan));

        let ctx = Arc::new(LocalModeContext::new());

        // create some executors
        let executors: Vec<Arc<dyn Executor>> = vec![
            Arc::new(DefaultExecutor::try_new(ctx.clone())?),
            Arc::new(DefaultExecutor::try_new(ctx.clone())?),
        ];

        let job = create_job(plan)?;
        let results = execute_job(&job, ctx.clone()).await?;

        assert_eq!(1, results.len());

        let batch = &results[0];

        assert_eq!(3961, batch.num_rows());
        assert_eq!(2, batch.num_columns());

        Ok(())
    })
}
