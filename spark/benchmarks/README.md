# Ballista Spark Benchmarks

These benchmarks exist so that we can measure relative performance between Ballista and 
Apache Spark for the benchmarks derived from TPC-H.

```bash
tpch --input-path /mnt/tpch/parquet-sf1 --input-format parquet --query-path ../../rust/benchmarks/tpch/queries --query 1
```