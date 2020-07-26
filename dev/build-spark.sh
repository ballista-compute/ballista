#!/bin/bash

BALLISTA_VERSION=0.3.0-alpha-1

set -e

pushd spark
./gradlew clean assemble
popd

docker build -t ballistacompute/ballista-spark:$BALLISTA_VERSION -f docker/spark-executor.dockerfile spark/executor
docker build -t ballistacompute/spark-benchmarks:$BALLISTA_VERSION -f docker/spark-benchmarks.dockerfile spark/benchmarks


