extern crate ballista;

use ballista::error::Result;
use ballista::execution::parquet_scan::ParquetStream;
use ballista::execution::physical_plan::ColumnarBatchStream;
use futures::StreamExt;

#[tokio::test]
async fn async_query() -> Result<()> {
    let path = nyc_path();
    let exec = ParquetStream::try_new(&path, None)?;
    let mut stream: ColumnarBatchStream = Box::pin(exec);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!(
            "batch with {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    Ok(())
}

fn nyc_path() -> String {
    //TODO use env var for path
    "/mnt/nyctaxi/parquet/year=2019/month=01/yellow_tripdata_2019-01.parquet/part-00000-794b9684-a630-438d-b75a-3adc80c85a7d-c000.snappy.parquet".to_owned()
}
