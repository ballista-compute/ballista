# Ballista Benchmarks

This directory contains code to run benchmarks in Docker and collect results. The latest published benchmarks can be found [here](https://ballistacompute.org/benchmarks/).

## Usage
 
```bash
cargo run -- --bench=spark --format=csv --path=/mnt/nyctaxi/csv/year=2019 --cpus=12
cargo run -- --bench=rust  --format=csv --path=/mnt/nyctaxi/csv/year=2019 --cpus=12
cargo run -- --bench=jvm   --format=csv --path=/mnt/nyctaxi/csv/year=2019 --cpus=12
``` 

