#!/bin/bash
BALLISTA_VERSION=0.3.0-SNAPSHOT
docker build -t ballistacompute/rust-cached-deps:$BALLISTA_VERSION -f docker/rust-cached-deps.dockerfile .
