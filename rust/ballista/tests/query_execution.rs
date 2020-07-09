extern crate ballista;

// use ballista::execution::hash_aggregate::HashAggregateExec;
// use ballista::execution::parquet_scan::ParquetScanExec;
// use ballista::execution::physical_plan::{AggregateMode, ColumnarBatchStream, PhysicalPlan};
//
// use ballista::dataframe::max;
// use datafusion::logicalplan::col_index;
// use std::rc::Rc;
//
// #[test]
// fn hash_aggregate() -> std::io::Result<()> {
//     smol::run(async {
//         let path = nyc_path();
//
//         let parquet = PhysicalPlan::ParquetScan(Rc::new(
//             ParquetScanExec::try_new(&path, Some(vec![3, 10])).unwrap(),
//         ));
//         println!("{:?}", parquet.as_execution_plan().schema());
//
//         let hash_agg = PhysicalPlan::HashAggregate(Rc::new(
//             HashAggregateExec::try_new(
//                 AggregateMode::Partial,
//                 vec![col_index(0)],
//                 vec![max(col_index(1))],
//                 Rc::new(parquet),
//             )
//             .unwrap(),
//         ));
//         println!("{:?}", hash_agg.as_execution_plan().schema());
//
//         let stream: ColumnarBatchStream = hash_agg.as_execution_plan().execute(0).unwrap();
//         while let Some(batch) = stream.next().await.unwrap() {
//             println!(
//                 "batch with {} rows and {} columns",
//                 batch.num_rows(),
//                 batch.num_columns()
//             );
//             println!("{:?}", batch);
//         }
//         std::io::Result::Ok(())
//     })
// }
//
// fn nyc_path() -> String {
//     //TODO use env var for path
//     "/mnt/nyctaxi/parquet/year=2019".to_owned()
// }
