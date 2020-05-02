#!/bin/bash

set -e

pushd spark
./gradlew clean assemble
popd

docker build -t ballistacompute/ballista-spark:0.2.4-SNAPSHOT -f docker/spark-executor.dockerfile spark


