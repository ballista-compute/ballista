#!/bin/bash
BALLISTA_VERSION=0.4.2-SNAPSHOT
set -e
docker build -t ballistacompute/rust-base:$BALLISTA_VERSION -f docker/rust-base.dockerfile .
