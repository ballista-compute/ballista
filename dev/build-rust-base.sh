#!/bin/bash
BALLISTA_VERSION=0.2.5-SNAPSHOT
docker build -t ballistacompute/rust-base:$BALLISTA_VERSION -f docker/rust-base.dockerfile .
