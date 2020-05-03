#!/bin/bash

set -e

pushd jvm
./gradlew clean assemble publishToMavenLocal
popd

docker build -t ballistacompute/ballista-jvm:0.2.4-SNAPSHOT -f docker/jvm-executor.dockerfile jvm/executor
docker build -t ballistacompute/jvm-benchmarks:0.2.4-SNAPSHOT -f docker/jvm-benchmarks.dockerfile jvm/benchmarks

