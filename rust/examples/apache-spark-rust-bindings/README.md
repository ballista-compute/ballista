# Rust bindings for Apache Spark

This example demonstrates using the Ballista Rust DataFrame to execute transformations and actions using Apache Spark.

See the top-level [Ballista README](../../../README.md) for an overview of the Ballista architecture, but the brief overview of this example is:

- Rust client uses Ballista DataFrame API to build a logical query plan
- Query plan is encoded in Ballista protobuf format and sent to a Scala executor process
- The executor translates the query plan into a Spark query and executes it
- Results are returned to the client via Apache Arrow Flight protocol

## Rust Client

The Rust client uses the Ballista DataFrame API to define a logical query plan. In this example the query is a simple aggregate query against a CSV file.

```rust
let spark_master = "local[*]";

let mut spark_settings = HashMap::new();
spark_settings.insert("spark.app.name", "rust-client-demo");
spark_settings.insert("spark.ballista.host", "localhost");
spark_settings.insert("spark.ballista.port", "50051");
spark_settings.insert("spark.executor.memory", "4g");
spark_settings.insert("spark.executor.cores", "4");

let ctx = Context::spark(spark_master, spark_settings);

let path = "/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv";

let df = ctx
  .read_csv(path, Some(nyctaxi_schema()), None, true)?
  .aggregate(
     vec![col("passenger_count")], 
     vec![min(col("fare_amount")), max(col("fare_amount"))])?;

// print the query plan
df.explain();

// collect the results from the Spark executor
let results = df.collect().await?;

// display the results
utils::print_batches(&results)?;
```

The Rust client produces the following output:

```
Aggregate: groupBy=[[#passenger_count]], aggr=[[MIN(#fare_amount), MAX(#fare_amount)]]
  TableScan: /mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv projection=None
+-----------------+-------+-----------+
| passenger_count | MIN   | MAX       |
+-----------------+-------+-----------+
| 1               | -362  | 623259.86 |
| 6               | -52   | 262.5     |
| 3               | -100  | 350       |
| 5               | -52   | 760       |
| 9               | 9     | 92        |
| 4               | -52   | 500       |
| 8               | 7     | 87        |
| 7               | -75   | 78        |
| 2               | -320  | 492.5     |
| 0               | -52.5 | 36090.3   |
+-----------------+-------+-----------+
```

## Spark Executor

The Ballista Spark Executor receives the protobuf-encoded logical query plan and translates it into the following Spark execution plan.

```
== Physical Plan ==
*(2) HashAggregate(keys=[passenger_count#3], functions=[min(fare_amount#10), max(fare_amount#10)])
+- Exchange hashpartitioning(passenger_count#3, 200), true, [id=#18]
   +- *(1) HashAggregate(keys=[passenger_count#3], functions=[partial_min(fare_amount#10), partial_max(fare_amount#10)])
      +- *(1) Project [passenger_count#3, fare_amount#10]
         +- BatchScan[passenger_count#3, fare_amount#10] CSVScan Location: InMemoryFileIndex[file:/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv], ReadSchema: struct<passenger_count:int,fare_amount:double>
```
