# TPC-H Benchmarks

TPC-H is an industry standard benchmark for testing databases and query engines. A command-line tool is available that
can generate the raw test data at any given scale factor (scale factor refers to the amount of data to be generated).

## Generating Test Data

TPC-H data can be generated using the `tpch-gen.sh` script, which creates a Docker image containing the TPC-DS data
generator.

```bash
./tpch-gen.sh
```

Data will be generated into the `data` subdirectory and will not be checked in because this directory has been added 
to the `.gitignore` file.

## Running the Benchmarks

To run the benchmarks it is necessary to have at least one Ballista executor running. The goal is that the benchmarks
can be run against either the Rust, JVM, or Spark executor, but currently only the Rust executor is supported.

To start a Rust executor from source:

```bash
cd $BALLISTA_HOME/rust/ballista
RUST_LOG=info cargo run --release --bin executor
```

To start a Rust executor using Docker Compose:

```bash
cd $BALLISTA_HOME
./dev/build-rust.sh
cd $BALLISTA_HOME/benchmarks/tpch
docker-compose up
```

To run the benchmarks:

```bash
cargo run benchmark --host localhost --port 50051 --query 12 --path data --format tbl
```

