#!/bin/bash
BALLISTA_VERSION=0.2.6-SNAPSHOT
docker build -t ballistacompute/rust-cached-deps:$BALLISTA_VERSION -f docker/rust-cached-deps.dockerfile .
