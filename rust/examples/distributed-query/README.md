# Distributed Query Execution

*NOTE: this is a work-in-progress and is not functional yet*

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query using those executors.

## Prerequisites

You will need to create a Ballista cluster in Kubernetes. This is documented in the [README](../../../kubernetes/README.md) in the top-level kubernetes folder.

## Execute the query

The example will create a logical query plan and submit it to the cluster for execution. The executor receiving the 
query will create a physical plan and schedule execution in the cluster and then return the results.

```bash
cargo run
``` 

## Teardown

Remove cluster:

```bash
kubectl delete -f parallel-aggregate-rs.yaml
kubectl delete -f cluster-deployment.yaml
```