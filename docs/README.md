# Ballista Developer Documentation

This directory contains documentation for developers that are contributing to Ballista. If you are looking for 
end-user documentation for a published release, please start with the [user guide](https://ballistacompute.org/docs/).

## Contributing to the Rust project

- Setting up a [development environment](dev-env-rust.md).
- [How to test the Rust executor](testing-rust-executor.md).
- How to [build the Rust docker image](rust-docker.md).

## Contributing to the JVM project

- Setting up a [development environment](dev-env-jvm.md).

## Test Data

- Where to find [NYC Taxi](nyctaxi.md) data.
- How to generate [TPCH](tpch.md) data.

## Testing

- [Running the integration test](integration-testing.md).

## Benchmarks

The goal is to be able to run the complete [TPC-H](tpch.md) benchmark and use that for comparisons with other 
distributed platforms, but Ballista can't support many of the queries yet.

The [NYC Taxi](nyctaxi.md) data set can also be used for running some simple aggregate queries and is easier to get up and 
running with because the source data can be downloaded from S3.

## Release

- [Release process](release-process.md)



