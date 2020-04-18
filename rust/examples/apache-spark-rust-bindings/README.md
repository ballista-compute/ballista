# Rust bindings for Apache Spark

This example demonstrates using the Ballista Rust DataFrame to execute transformations and actions using Apache Spark.

See the top-level [Ballista README](../../../README.md) for an overview of the Ballista architecture, but the brief overview of this example is:

- Rust client uses Ballista DataFrame API to build a logical query plan
- Query plan is encoded in Ballista protobuf format and sent to a Scala executor process
- The executor translates the query plan into a Spark query and executes it
- Results are returned to the client via Apache Arrow Flight protocol

## Example

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
    .aggregate(vec![col("passenger_count")], vec![min(col("fare_amount")), max(col("fare_amount"))])?;

// print the query plan
df.explain();

// collect the results from the Spark executor
let results = df.collect().await?;

// display the results
utils::print_batches(&results)?;
```

## Output

Rust Client:

```
Aggregate: groupBy=[[#passenger_count]], aggr=[[MAX(#fare_amount)]]
  TableScan: /mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv projection=None
+-----------------+-------------+
| passenger_count | MAX         |
+-----------------+-------------+
| 7               | 78          |
| 3               | 99.75       |
| 8               | 9           |
| 0               | 99          |
| 5               | 99.5        |
| 6               | 98          |
| 9               | 92          |
| 1               | 99.99       |
| 4               | 99          |
| 2               | 99.75       |
+-----------------+-------------+
```

Spark Executor:

```
== Physical Plan ==
SortAggregate(key=[passenger_count#3], functions=[max(fare_amount#10)])
+- *(2) Sort [passenger_count#3 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(passenger_count#3, 200), true, [id=#23]
      +- SortAggregate(key=[passenger_count#3], functions=[partial_max(fare_amount#10)])
         +- *(1) Sort [passenger_count#3 ASC NULLS FIRST], false, 0
            +- *(1) Project [passenger_count#3, fare_amount#10]
               +- BatchScan[passenger_count#3, fare_amount#10] CSVScan Location: InMemoryFileIndex[file:/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv], ReadSchema: struct<passenger_count:string,fare_amount:string>

```
