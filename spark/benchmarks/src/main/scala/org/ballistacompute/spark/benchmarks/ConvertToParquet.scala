// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.spark.benchmarks

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.io.File

/**
  * Utility for converting CSV to Parquet.
  */
object ConvertToParquet {

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
