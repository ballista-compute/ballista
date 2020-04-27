# JVM Query Engine

## Overview


## Building Executor Docker Image

```bash
cd $BALLISTA_HOME/jvm
./gradlew clean
./gradlew assemble
cd executor
docker build -t ballista-jvm .
```
