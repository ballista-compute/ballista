package org.ballistacompute.spark.benchmarks.nyctaxi

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.io.File

/**
  * Utility for converting CSV to Parquet.
  */
object NYCTaxi {

  def main(arg: Array[String]): Unit = {
    for (year <- 2019 to 2019) {
      for (month <- 1 to 12) {
        val monthStr = "%02d".format(month)
        val csvPath =
          s"/mnt/nyctaxi/csv/year=$year/yellow_tripdata_$year-$monthStr.csv"
        val parquetPath =
          s"/mnt/nyctaxi/parquet/year=$year/month=$monthStr/yellow_tripdata_$year-$monthStr.parquet"
        if (File(parquetPath).exists) {
          println(s"$parquetPath exists")
        } else {
          println(s"Creating $parquetPath")
          convertToParquet(csvPath, parquetPath)
        }
      }
    }
  }

  /** Load CSV with schema inferred so that parquet file has correct types */
  def convertToParquet(csvPath: String, parquetPath: String) {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)

    df.printSchema()

  }

}
