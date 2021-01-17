#!/bin/bash

BALLISTA_VERSION=0.4.0-SNAPSHOT

set -e

# Rust proto is ahead of top level proto - see https://github.com/ballista-compute/ballista/issues/374
#cp -f proto/ballista.proto rust/ballista/proto/

docker build -t ballistacompute/ballista-rust:$BALLISTA_VERSION -f docker/rust.dockerfile .
