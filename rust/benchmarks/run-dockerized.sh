#!/usr/bin/env bash

# run via docker
docker run --cpus=12 -e BENCH_RESULT_FILE="rust-dockerized.txt" -e BENCH_PATH="/mnt/nyctaxi" -e BENCH_FORMAT="csv" -e BENCH_ITERATIONS="3" -e BENCH_MODE="local" -v /mnt/nyctaxi/csv/year=2019:/mnt/nyctaxi -v /home/andy/git/andygrove/ballista/rust/benchmarks:/results ballistacompute/rust-benchmarks:0.2.5
