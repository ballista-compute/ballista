# Spark Executor

## Overview


## Building Spark Executor Docker Image

```bash
cd $BALLISTA_HOME/spark
./gradlew clean
./gradlew assemble
cd executor
docker build -t ballista-jvm .
```
