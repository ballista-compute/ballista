#!/bin/bash

BALLISTA_VERSION=0.2.6-SNAPSHOT

set -e

pushd jvm
./gradlew clean assemble publishToMavenLocal
popd

docker build -t ballistacompute/ballista-jvm:$BALLISTA_VERSION -f docker/jvm-executor.dockerfile jvm/executor
docker build -t ballistacompute/jvm-benchmarks:$BALLISTA_VERSION -f docker/jvm-benchmarks.dockerfile jvm/benchmarks

