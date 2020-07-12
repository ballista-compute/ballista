# Distributed Query Execution

This example demontrates running a distributed query against a local Ballista cluster, using etcd for discovery.

## Prerequisites

etcd must be running locally.

## Start one or more executors

```bash
cargo run --release --bin executor -- --mode=etcd
```

## Execute the query

The example will create a logical query plan and submit it to the cluster for execution. The executor receiving the 
query will create a physical plan and schedule execution in the cluster and then return the results.

```bash
cargo run
``` 

