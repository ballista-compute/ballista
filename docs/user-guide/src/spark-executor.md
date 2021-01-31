# Spark Executor

## Overview

The Ballista Spark Executor executes Ballista query plans using Apache Spark. The executor is a Spark Driver that is hosting a Ballista server. It is conceptually similar to using the Spark SQL Thrift Server.

This executor can be run as a standalone process, in which case it runs as a Spark Driver either in standalone or client mode, depending on the value passed for the spark master property. The executor can also be deployed to in cluster mode using `spark-submit`.

## Benefits

One benefit of this executor is that it allows Spark to be used from Rust. See the [Apache Spark Rust Bindings](https://github.com/ballista-compute/ballista/tree/main/rust/examples/apache-spark-rust-bindings) example for more information.

Another benefit is that it allows Spark to be used in a distributed compute pipeline alongside other executors.

## Building from source

Run the following command to build the Spark module and create a Docker image containing the Ballista Spark Executor.

```bash
./dev/build-spark.sh
```
