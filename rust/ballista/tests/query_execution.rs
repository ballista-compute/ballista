extern crate ballista;

// use ballista::error::Result;
// use ballista::execution::hash_aggregate::HashAggregateExec;
// use ballista::execution::parquet_scan::ParquetScanExec;
// use ballista::execution::physical_plan::{
//     AggregateMode, ColumnarBatch, ColumnarBatchStream, PhysicalPlan,
// };
//
// use futures::prelude::*;
// use std::env;
// use std::rc::Rc;

// #[test]
// fn hash_aggregate() -> std::io::Result<()> {
//     smol::run(async {
//         let path = nyc_path();
//         let parquet = PhysicalPlan::ParquetScan(Rc::new(ParquetScanExec::try_new(&path, None).unwrap()));
//         let hash_agg = PhysicalPlan::HashAggregate(Rc::new(HashAggregateExec::new(
//             AggregateMode::Partial,
//             vec![],
//             vec![],
//             Rc::new(parquet),
//         )));
//
//         let stream: ColumnarBatchStream = hash_agg.as_execution_plan().execute(0).unwrap();
//         while let Some(batch) = stream.next().await.unwrap() {
//             println!(
//                 "batch with {} rows and {} columns",
//                 batch.num_rows(),
//                 batch.num_columns()
//             );
//         }
//         std::io::Result::Ok(())
//     })
// }
//
// fn nyc_path() -> String {
//     //TODO use env var for path
//     "/mnt/nyctaxi/parquet/year=2019".to_owned()
// }
