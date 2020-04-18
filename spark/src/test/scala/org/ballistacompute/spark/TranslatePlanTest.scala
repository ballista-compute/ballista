package org.ballistacompute.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.spark.executor.BallistaSparkContext
import org.ballistacompute.{logical => ballista}
import org.junit.{Ignore, Test}

import scala.collection.JavaConverters._

class TranslatePlanTest {

  @Test
  @Ignore
  def sanityCheck(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("VendorID", DataTypes.StringType, true),
      StructField("tpep_pickup_datetime", DataTypes.StringType, true),
      StructField("tpep_dropoff_datetime", DataTypes.StringType, true),
      StructField("passenger_count", DataTypes.IntegerType, true),
      StructField("trip_distance", DataTypes.StringType, true),
      StructField("RatecodeID", DataTypes.StringType, true),
      StructField("store_and_fwd_flag", DataTypes.StringType, true),
      StructField("PULocationID", DataTypes.StringType, true),
      StructField("DOLocationID", DataTypes.StringType, true),
      StructField("payment_type", DataTypes.StringType, true),
      StructField("fare_amount", DataTypes.FloatType, true),
      StructField("extra", DataTypes.FloatType, true),
      StructField("mta_tax", DataTypes.FloatType, true),
      StructField("tip_amount", DataTypes.FloatType, true),
      StructField("tolls_amount", DataTypes.FloatType, true),
      StructField("improvement_surcharge", DataTypes.FloatType, true),
      StructField("total_amount", DataTypes.FloatType, true)
    ))

    val df = spark.read
      .format("csv")
      .schema(schema)
      .load("/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv")
      .groupBy("passenger_count")
      .agg(min(col("fare_amount")), max(col("fare_amount")))
//      .agg(min(col("fare_amount")).alias("min_fare"), max(col("fare_amount")).alias("max_fare"))

    run(df)
  }

  def run(df: DataFrame): Unit = {
    df.explain()
    df.collect().foreach(println)
  }


  @Test
  @Ignore
  def testSomething() {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val filename = "/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv"

    val projection = List[String]().asJava

    val plan = new ballista.DataFrameImpl(new ballista.Scan(filename, new CsvDataSource(filename, null, 1024), projection))
      .filter(new ballista.Eq(new ballista.Column("_c0"), new ballista.LiteralString("foo")))
      .project(List[ballista.LogicalExpr](new ballista.Column("_c1")).asJava)
      .logicalPlan()

    println(plan.pretty())

    val ctx = new BallistaSparkContext(spark)
    val df = ctx.createDataFrame(plan, None)
    df.explain()
  }
}
