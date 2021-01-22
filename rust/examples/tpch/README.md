# Rust TPC-H benchmark client

This example is forked from the 
[Apache Arrow Rust Benchmark crate](https://github.com/apache/arrow/tree/master/rust/benchmarks) and instructions for
generating TPC-H data sets can be found there.

```bash
cargo run benchmark --host localhost --port 50051 --query 12 --path /mnt/tpch/ --format parquet
```

