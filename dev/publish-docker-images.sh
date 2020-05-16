#!/bin/bash

BALLISTA_VERSION=0.2.6-SNAPSHOT

set -e

docker tag ballistacompute/ballista-rust:$BALLISTA_VERSION ballistacompute/ballista-rust:latest
docker tag ballistacompute/ballista-jvm:$BALLISTA_VERSION ballistacompute/ballista-jvm:latest
docker tag ballistacompute/ballista-spark:$BALLISTA_VERSION ballistacompute/ballista-spark:latest

docker tag ballistacompute/rust-benchmarks:$BALLISTA_VERSION ballistacompute/rust-benchmarks:latest
docker tag ballistacompute/jvm-benchmarks:$BALLISTA_VERSION ballistacompute/jvm-benchmarks:latest
docker tag ballistacompute/spark-benchmarks:$BALLISTA_VERSION ballistacompute/spark-benchmarks:latest

docker push ballistacompute/ballista-rust:$BALLISTA_VERSION
docker push ballistacompute/ballista-jvm:$BALLISTA_VERSION
docker push ballistacompute/ballista-spark:$BALLISTA_VERSION
docker push ballistacompute/ballista-rust:latest
docker push ballistacompute/ballista-jvm:latest
docker push ballistacompute/ballista-spark:latest

docker push ballistacompute/rust-benchmarks:$BALLISTA_VERSION
docker push ballistacompute/jvm-benchmarks:$BALLISTA_VERSION
docker push ballistacompute/spark-benchmarks:$BALLISTA_VERSION
docker push ballistacompute/rust-benchmarks:latest
docker push ballistacompute/jvm-benchmarks:latest
docker push ballistacompute/spark-benchmarks:latest
