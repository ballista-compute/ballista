# Executing queries from Rust

For this example we will be querying a 600 MB CSV file from the NYC Taxi and Limousine Commission public data set, which 
can be downloaded [here](https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-12.csv).

Create a new Rust binary application using `cargo init --bin nyctaxi` then add the following dependency section to the 
`Cargo.toml` file.

```toml
[dependencies]
ballista = "0.4.0-SNAPSHOT"
tokio = { version = "0.2", features = ["full"] }
```

Replace the `src/main.rs` with the following code

```rust
use std::collections::HashMap;

extern crate ballista;
use ballista::arrow::util::pretty;
use ballista::dataframe::{min, max, Context};
use ballista::datafusion::logicalplan::*;
use ballista::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::remote("localhost", 50051, HashMap::new());

    let results = ctx
        .read_csv(
            "yellow_tripdata_2019-12.csv", 
            CsvReadOptions::new().has_header(true))?
        .aggregate(
            vec![col("passenger_count")], 
            vec![min(col("fare_amount")), max(col("fare_amount"))])?
        .collect()
        .await?;

    pretty::print_batches(&results)?;

    Ok(())
}
```

Execute the code using `cargo run` and it should produce the following output.

```
+-----------------+--------------------------------
| passenger_count | MIN(fare_amt) | MAX(fare_amt) |
+-----------------+--------------------------------
| 9               | -90           | 98            |
| 0               | -78           | 900           |
| 3               | -280          | 499           |
| 4               | -250          | 700           |
| 7               | 0.77          | 78            |
| 8               | 8             | 88            |
| 2               | -1472         | 1472          |
| 6               | -65           | 544.5         |
| 1               | -600          | 398468.38     |
| 5               | -52           | 442           |
+-----------------+---------------+---------------+
```

