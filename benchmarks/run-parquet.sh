#!/usr/bin/env bash
cargo run -- --bench=spark --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=3
cargo run -- --bench=rust --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=3
#cargo run -- --bench=jvm --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=3

cargo run -- --bench=spark --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=6
cargo run -- --bench=rust --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=6
#cargo run -- --bench=jvm --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=6

cargo run -- --bench=spark --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=9
cargo run -- --bench=rust --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=9
#cargo run -- --bench=jvm --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=9

cargo run -- --bench=spark --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=12
cargo run -- --bench=rust --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=12
#cargo run -- --bench=jvm --format=parquet --path=/mnt/nyctaxi/parquet/year=2019 --cpus=12