package org.ballistacompute.benchmarks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

object Benchmarks {

  def run(format: String, path: String, sql: String, iterations: Int): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]") // use as many threads as needed
      .getOrCreate()

    format match {
      case "parquet" => loadParquet(spark, path)
      case "csv" => loadCsv(spark, path)
      case other =>
        println(s"Invalid format: $other")
        System.exit(1)
    }


    val durations = for (i <- 1 to iterations) yield {
      println(s"**** Running iteration $i")
      val t1 = System.currentTimeMillis()
      val df = spark.sql(sql)
      df.explain()
      df.collect().foreach(println)
      val t2 = System.currentTimeMillis()
      (t2-t1)
    }

    spark.close()

    durations.zipWithIndex.foreach {
      case (duration,iter) =>
        println(s"Iteration ${iter+1} took ${duration/1000.0} seconds")
    }

  }

  def loadCsv(spark: SparkSession, path: String) {

    val schema = StructType(Seq(
      StructField("VendorID", DataTypes.IntegerType),
      StructField("tpep_pickup_datetime", DataTypes.TimestampType),
      StructField("tpep_dropoff_datetime", DataTypes.TimestampType),
      StructField("passenger_count", DataTypes.IntegerType),
      StructField("trip_distance", DataTypes.DoubleType),
      StructField("RatecodeID", DataTypes.IntegerType),
      StructField("store_and_fwd_flag", DataTypes.StringType),
      StructField("PULocationID", DataTypes.IntegerType),
      StructField("DOLocationID", DataTypes.IntegerType),
      StructField("payment_type", DataTypes.IntegerType),
      StructField("fare_amount", DataTypes.DoubleType),
      StructField("extra", DataTypes.DoubleType),
      StructField("mta_tax", DataTypes.DoubleType),
      StructField("tip_amount", DataTypes.DoubleType),
      StructField("tolls_amount", DataTypes.DoubleType),
      StructField("improvement_surcharge", DataTypes.DoubleType),
      StructField("total_amount", DataTypes.DoubleType)
    ))
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(path)

    df.printSchema()
    df.createOrReplaceTempView("tripdata")
  }

  def loadParquet(spark: SparkSession, path: String) {
    val df = spark.read
      .parquet(path)

    df.printSchema()
    df.createOrReplaceTempView("tripdata")
  }


}
