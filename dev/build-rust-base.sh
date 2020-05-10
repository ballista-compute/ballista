#!/bin/bash
BALLISTA_VERSION=0.2.5
docker build -t ballistacompute/rust-base:$BALLISTA_VERSION -f docker/rust-base.dockerfile .
