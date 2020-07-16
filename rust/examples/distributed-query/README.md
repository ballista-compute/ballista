# Distributed Query Execution

This example demonstrates running a distributed query against a local Ballista cluster. The example builds a logical
query plan using a DataFrame style API and then calls `collect` to trigger execution of the query. The logical query
plan is submitted to an executor which then takes on the role of scheduler and orchestrates execution of the query 
across the cluster.

```rust
let ctx = Context::remote("localhost", 50051, HashMapk8s_get_executors::new());

let results = ctx
    .read_parquet(nyc_taxi_path, None)?
    .aggregate(vec![col("passenger_count")], vec![max(col("fare_amount"))])?
    .collect()
    .await?;
```

## Creating a Ballista Cluster

Follow the [installation instructions](https://ballistacompute.org/docs/installation.html) in the user guide to create 
a local or distributed Ballista cluster to test against.

## Run the example

The example will create a logical query plan and submit it to the cluster for execution. The executor receiving the 
query will create a physical plan and schedule execution in the cluster and then return the results.

```bash
cargo run
``` 
