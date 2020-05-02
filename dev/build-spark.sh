#!/bin/bash

set -e

pushd spark
./gradlew clean assemble
popd

docker build -t ballistacompute/ballista-spark -f docker/spark-executor.dockerfile spark


