#!/bin/bash

BALLISTA_VERSION=0.2.5

set -e

pushd rust
cargo fmt
cargo test
popd

docker build -t ballistacompute/ballista-rust:$BALLISTA_VERSION -f docker/rust-executor.dockerfile .
docker build -t ballistacompute/rust-benchmarks:$BALLISTA_VERSION -f docker/rust-benchmarks.dockerfile .
