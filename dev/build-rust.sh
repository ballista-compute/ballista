#!/bin/bash

set -e

pushd rust
cargo fmt
cargo test
popd

docker build -t ballistacompute/ballista-rust:0.2.4-SNAPSHOT -f docker/rust-executor.dockerfile .
docker build -t ballistacompute/rust-benchmarks:0.2.4-SNAPSHOT -f docker/rust-benchmarks.dockerfile .
