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

You can add SIMD/snmalloc/LTO flags to improve speed (with longer build times):

```
RUST_LOG=info RUSTFLAGS='-C target-cpu=native -C lto -C codegen-units=1 -C embed-bitcode' cargo run --release --bin executor --features "simd snmalloc" --target x86_64-unknown-linux-gnu
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
cargo run benchmark --host localhost --port 50051 --query 1 --path data --format tbl
```

This should produce the following output:

```
+--------------+--------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+----------------------+-------------+
| l_returnflag | l_linestatus | sum_qty  | sum_base_price     | sum_disc_price     | sum_charge         | avg_qty            | avg_price          | avg_disc             | count_order |
+--------------+--------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+----------------------+-------------+
| A            | F            | 37734107 | 56586554400.73001  | 53758257134.870026 | 55909065222.82768  | 25.522005853257337 | 38273.12973462168  | 0.049985295838396455 | 1478493     |
| N            | F            | 991417   | 1487504710.3799996 | 1413082168.0541    | 1469649223.1943746 | 25.516471920522985 | 38284.467760848296 | 0.05009342667421622  | 38854       |
| N            | O            | 74476023 | 111701708529.50996 | 106118209986.10472 | 110367023144.56622 | 25.502229680934594 | 38249.1238377803   | 0.049996589476752576 | 2920373     |
| R            | F            | 37719753 | 56568041380.90001  | 53741292684.60399  | 55889619119.83194  | 25.50579361269077  | 38250.854626099666 | 0.05000940583012587  | 1478870     |
+--------------+--------------+----------+--------------------+--------------------+--------------------+--------------------+--------------------+----------------------+-------------+
Query 1 iteration 0 took 1956.1 ms
Query 1 avg time: 1956.11 ms
```
