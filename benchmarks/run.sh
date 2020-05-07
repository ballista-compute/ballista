#!/usr/bin/env bash
cargo run -- --bench=spark --path=/mnt/nyctaxi/csv/year=2019 --cpus=1
cargo run -- --bench=rust --path=/mnt/nyctaxi/csv/year=2019 --cpus=1
cargo run -- --bench=jvm --path=/mnt/nyctaxi/csv/year=2019 --cpus=1

cargo run -- --bench=spark --path=/mnt/nyctaxi/csv/year=2019 --cpus=3
cargo run -- --bench=rust --path=/mnt/nyctaxi/csv/year=2019 --cpus=3
cargo run -- --bench=jvm --path=/mnt/nyctaxi/csv/year=2019 --cpus=3

cargo run -- --bench=spark --path=/mnt/nyctaxi/csv/year=2019 --cpus=6
cargo run -- --bench=rust --path=/mnt/nyctaxi/csv/year=2019 --cpus=6
cargo run -- --bench=jvm --path=/mnt/nyctaxi/csv/year=2019 --cpus=6

cargo run -- --bench=spark --path=/mnt/nyctaxi/csv/year=2019 --cpus=9
cargo run -- --bench=rust --path=/mnt/nyctaxi/csv/year=2019 --cpus=9
cargo run -- --bench=jvm --path=/mnt/nyctaxi/csv/year=2019 --cpus=9

cargo run -- --bench=spark --path=/mnt/nyctaxi/csv/year=2019 --cpus=12
cargo run -- --bench=rust --path=/mnt/nyctaxi/csv/year=2019 --cpus=12
cargo run -- --bench=jvm --path=/mnt/nyctaxi/csv/year=2019 --cpus=12