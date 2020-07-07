extern crate ballista;

use ballista::error::Result;
use ballista::execution::parquet_scan::ParquetScanExec;
use ballista::execution::physical_plan::{ColumnarBatchStream, ExecutionPlan};
use futures::StreamExt;

// #[tokio::test]
// async fn async_query() -> Result<()> {
//     let path = nyc_path();
//     let exec = ParquetScanExec::try_new(&path, None)?;
//     let mut stream: ColumnarBatchStream = exec.execute(0)?;
//     while let Some(batch) = stream.next().await {
//         let batch = batch?;
//         println!(
//             "batch with {} rows and {} columns",
//             batch.num_rows(),
//             batch.num_columns()
//         );
//     }
//     Ok(())
// }

fn nyc_path() -> String {
    //TODO use env var for path
    "/mnt/nyctaxi/parquet/year=2019".to_owned()
}
