#!/bin/bash
BALLISTA_VERSION="$1"
docker push ballistacompute/rust-base:$BALLISTA_VERSION
docker push ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker push ballistacompute/rust:$BALLISTA_VERSION
