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

import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Benchmarks {

  def main(arg: Array[String]): Unit = {
    queryUsingDataFrame()
  }

  def queryUsingDataFrame(): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Example")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.parquet("/mnt/nyctaxi/parquet")
      .groupBy("passenger_count")
      .sum("fare_amount")
      .orderBy("passenger_count")

    df.show()

  }

  def run(format: String, path: String, sql: String, iterations: Int, outputFile: String): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master(s"local[*]")
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

    println(s"Writing results to $outputFile")
    val w = new FileWriter(new File(outputFile))
    w.write("iteration,time_millis\n")
    durations.zipWithIndex.foreach {
      case (duration,iter) =>
        println(s"Iteration ${iter+1} took ${duration/1000.0} seconds")
        w.write(s"${iter+1},$duration\n")
    }
    w.close()

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
