# Apache Spark Benchmarks for NYC trip data

## Running from Docker

```bash
./build-docker-image.sh
docker run --cpus="12" --memory="8g" -v /mnt/nyctaxi:/mnt/nyctaxi -it ballistacompute/spark-benchmarks ./bin/spark-benchmarks bench parquet /mnt/nyctaxi/parquet/2018 "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count" 5
```

## Convert single CSV to Parquet

Converts a single CSV file to a Parquet file.

```bash
./gradlew run --args='convert yellow_tripdata_2010-01.csv yellow_tripdata_2010-01.parquet'
```

## Run benchmarks with Gradle

### Parquet

```bash
./gradlew run --args='bench parquet /mnt/nyctaxi/parquet "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM tripdata GROUP BY passenger_count" 5'
```

### CSV

```bash
./gradlew run --args='bench csv /mnt/nyctaxi "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM tripdata GROUP BY passenger_count" 5'
```

## Rust REST API

```bash
./gradlew run --args='server'
```

# Status

- [ ] In-process
- [ ] REST single process (spark context per query)
- [ ] REST single process (shared spark context)
- [ ] Kubernetes cluster

# Queries and expected results

```sql
SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata GROUP BY passenger_count
```

```
[192,6.0,6.0]
[1,-800.0,907070.24]
[6,-100.0,433.0]
[3,-498.0,349026.72]
[96,6.0,6.0]
[5,-300.0,1271.5]
[9,0.09,110.0]
[4,-415.0,974.5]
[8,-89.0,129.5]
[7,-70.0,140.0]
[2,-498.0,214748.44]
[0,-90.89,40502.24]
```

