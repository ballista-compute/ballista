# Ballista JVM Libraries

This project contains the following components:

- Ballista JVM Query Engine
- Ballista JDBC Driver for executing queries against a Ballista cluster
- Ballista Spark V2 Data Source that allows Spark to execute queries against a Ballista cluster

## Installing Locally

```bash
./gradlew publishToMavenLocal
``` 

## Sample Data

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-12.csv
```