# Ballista integration with Apache Spark

This project contains:

- Spark benchmarks and utilities for TPC-H and NYC Taxi
- Ballista Spark Executor (prototype)
- Spark V2 Connector for Ballista (prototype)

## Build

```bash
./gradlew assemble
```

## Formatting

```bash
./gradlew spotlessApply
```

## Build Benchmark Docker Image

```bash
docker build -t ballistacompute/spark-benchmarks:0.4.0-SNAPSHOT -f benchmarks/Dockerfile .
```

## Ballista Spark Executor

Executor implementing the Ballista protocol (Apache Flight + protobuf-encoded Ballista query plans), allowing Spark to 
be used from any language supported by Ballista, including Java, Kotlin, Scala, and Rust.

## Spark V2 Connector for Ballista

The goal for this component is to allow Spark to interact with Ballista executors (Rust, Kotlin, and Spark executors 
exist).

## Running benchmarks on k8s

# Ballista Spark Benchmarks

## Prerequisites

Follow instructions at http://spark.apache.org/docs/latest/running-on-kubernetes.html

Relies on rbac and pv from top-level kubernetes dir in this repo

## Build Jars

```bash
./gradlew assemble
```

## Build Docker Image

```bash
docker build -t ballistacompute/spark-benchmarks:0.4.0-SNAPSHOT .
```

## Deploy

```bash
export SPARK_HOME=/path/to/spark-3.0.0-bin-hadoop3.2
./run-tpch-k8s.sh
```
