# Ballista Spark Benchmarks

These benchmarks exist so that we can measure relative performance between Ballista and 
Apache Spark for the benchmarks derived from TPC-H.

## Pre-requisites

- Download Apache Maven from https://maven.apache.org/download.cgi
- Download Apache Spark 3.0.2 from https://spark.apache.org/downloads.html

Untar these downloads and set `MAVEN_HOME` and `SPARK_HOME` environment variables to point to the 
install location.

## Build the benchmark JAR file

```bash
$MAVEN_HOME/bin/mvn package
```

## Start a local Spark cluster in standalone mode

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://ripper:7077
```

## Submit the benchmark application to the cluster

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class org.ballistacompute.spark.benchmarks.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=8G \
    --conf spark.executor.cores=24 \
    target/spark-benchmarks-0.4.1-jar-with-dependencies.jar \
    tpch --input-path /mnt/tpch/parquet-sf100-partitioned \
    --input-format parquet \
    --query-path ../../rust/benchmarks/tpch/queries \
    --query 3
```

Monitor progress via the Spark UI at http://localhost:8080

## Shut down the cluster

```bash
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```