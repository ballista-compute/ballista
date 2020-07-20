#!/bin/bash
BALLISTA_VERSION=0.3.0-SNAPSHOT
docker build -t ballistacompute/rust-base:$BALLISTA_VERSION -f docker/rust-base.dockerfile .
