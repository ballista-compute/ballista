#!/bin/bash
BALLISTA_VERSION=0.2.5-SNAPSHOT
docker build -t ballistacompute/rust-cached-deps:BALLISTA_VERSION -f docker/rust-cached-deps.dockerfile .
