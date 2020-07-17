#!/bin/bash

BALLISTA_VERSION=0.3.0-SNAPSHOT

set -e

cp -f proto/ballista.proto rust/ballista/proto/

pushd rust
cargo fmt
cargo test
popd

docker build -t ballistacompute/ballista-rust:$BALLISTA_VERSION -f docker/rust-executor.dockerfile .
