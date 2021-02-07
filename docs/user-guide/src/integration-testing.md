# Integration Testing

*NOTE:* Ballista 0.4.0 is still under development and this page is out of date. Please see the [benchmarks](https://github.com/ballista-compute/ballista/tree/main/rust/benchmarks/tpch) for
current usage.

## Start the executors

The integration-tests directory contains a `docker-compose.yml` defining the three executors.

These executors can be started by running `docker-compose up` and can be terminated by running `docker-compose down`.

```
version: '2.0'
services:
  ballista-rust:
    image: ballistacompute/ballista-rust
    ports:
      - "50051:50051"
    volumes:
      - /mnt/nyctaxi:/mnt/nyctaxi
  ballista-jvm:
    image: ballistacompute/ballista-jvm
    ports:
      - "50052:50051"
    volumes:
      - /mnt/nyctaxi:/mnt/nyctaxi
  ballista-spark:
    image: ballistacompute/ballista-spark
    ports:
      - "50053:50051"
    volumes:
      - /mnt/nyctaxi:/mnt/nyctaxi
```

## Run the tests

Currently, there is a single integration test, implemented in Rust.

```bash
cd rust
cargo run
```
