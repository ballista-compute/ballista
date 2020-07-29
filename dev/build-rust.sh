#!/bin/bash

BALLISTA_VERSION=0.3.0-SNAPSHOT

set -e

cp -f proto/ballista.proto rust/ballista/proto/

docker build -f docker/rust.dockerfile -t ballistacompute/ballista-rust:latest .
