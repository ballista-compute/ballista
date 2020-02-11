package io.andygrove.ballista.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
  * Run this directly to run in local mode (in-process) or package as a JAR and use spark-submit
  * to run in client or cluster mode. See README for more information.
  */
object Main {

  def main(arg: Array[String]): Unit = {

    val dataDir = "/mnt/ssd/nyc_taxis/csv"

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val sql =
      """
        |SELECT passenger_count, MIN(fare_amount), MAX(fare_amount)
        |FROM tripdata
        |GROUP BY passenger_count
      """.stripMargin

    val schema = StructType(Array(
      StructField("VendorID", StringType, true),
      StructField("tpep_pickup_datetime", StringType, true),
      StructField("tpep_dropoff_datetime", StringType, true),
      StructField("passenger_count", IntegerType, true),
      StructField("trip_distance", StringType, true),
      StructField("RatecodeID", StringType, true),
      StructField("store_and_fwd_flag", StringType, true),
      StructField("PULocationID", StringType, true),
      StructField("DOLocationID", StringType, true),
      StructField("payment_type", StringType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("extra", DoubleType, true),
      StructField("mta_tax", DoubleType, true),
      StructField("tip_amount", DoubleType, true),
      StructField("tolls_amount", DoubleType, true),
      StructField("improvement_surcharge", DoubleType, true),
      StructField("total_amount", DoubleType, true)
    ))

    spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(dataDir)
      .createTempView("tripdata")

    val df = spark.sql(sql)

    // show the detailed distributed query plan
    df.explain(true)

    // execute the query and collect the results
    val result = df.collect()

    println(s"Result has ${result.length} rows:")
    result.foreach(println)

  }

}

