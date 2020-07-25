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
docker build -t ballistacompute/spark-benchmarks:0.3.0-SNAPSHOT .
```

## Deploy

```bash
export SPARK_HOME=/path/to/spark-3.0.0-bin-hadoop3.2
./run-tpch-k8s.sh
```

