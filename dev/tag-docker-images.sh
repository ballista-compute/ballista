#!/bin/bash
BALLISTA_VERSION="$1"
docker tag ballistacompute/rust-base ballistacompute/rust-base:$BALLISTA_VERSION
docker tag ballistacompute/rust-cached-deps ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker tag ballistacompute/rust ballistacompute/rust:$BALLISTA_VERSION
