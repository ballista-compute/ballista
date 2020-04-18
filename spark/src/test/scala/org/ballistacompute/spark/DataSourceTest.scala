package io.andygrove.ballista.spark

import org.junit.{Ignore, Test}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class DataSourceTest {

  @Test
  def sanityCheck(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      new StructField("VendorID", DataTypes.StringType, true),
      new StructField("tpep_pickup_datetime", DataTypes.StringType, true),
      new StructField("tpep_dropoff_datetime", DataTypes.StringType, true),
      new StructField("passenger_count", DataTypes.IntegerType, true),
      new StructField("trip_distance", DataTypes.StringType, true),
      new StructField("RatecodeID", DataTypes.StringType, true),
      new StructField("store_and_fwd_flag", DataTypes.StringType, true),
      new StructField("PULocationID", DataTypes.StringType, true),
      new StructField("DOLocationID", DataTypes.StringType, true),
      new StructField("payment_type", DataTypes.StringType, true),
      new StructField("fare_amount", DataTypes.FloatType, true),
      new StructField("extra", DataTypes.FloatType, true),
      new StructField("mta_tax", DataTypes.FloatType, true),
      new StructField("tip_amount", DataTypes.FloatType, true),
      new StructField("tolls_amount", DataTypes.FloatType, true),
      new StructField("improvement_surcharge", DataTypes.FloatType, true),
      new StructField("total_amount", DataTypes.FloatType, true)
    ))

    val df = spark.read
      .format("csv")
      .schema(schema)
      .load("/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv")
      .groupBy("passenger_count")

    def run(df: DataFrame): Unit = {
      df.explain()
      df.collect().foreach(println)
    }

    run(df.min("fare_amount"))
    run(df.max("fare_amount"))
  }

  @Test
  @Ignore
  def testSomething() {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("io.andygrove.ballista.spark.datasource")
      .option("table", "/home/andy/git/andygrove/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet")
      .option("host", "127.0.0.1")
      .option("port", "50051")
      .load()

    df.printSchema()

    val results = df.collect()
    results.foreach(println)

  }
}
