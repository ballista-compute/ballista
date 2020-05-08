#!/usr/bin/env bash
export BENCH_MODE=local

#export BENCH_FORMAT=csv
#export BENCH_PATH=/mnt/nyctaxi/csv/year=2019

export BENCH_FORMAT=parquet
export BENCH_PATH=/mnt/nyctaxi/parquet/year=2019

export BENCH_RESULT_FILE=rust-debug.txt

# run natively
cargo run --release

