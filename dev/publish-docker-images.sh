#!/bin/bash

BALLISTA_VERSION=0.4.2-SNAPSHOT

set -e

docker tag ballistacompute/ballista-rust:$BALLISTA_VERSION ballistacompute/ballista-rust:latest
docker push ballistacompute/ballista-rust:$BALLISTA_VERSION
docker push ballistacompute/ballista-rust:latest

