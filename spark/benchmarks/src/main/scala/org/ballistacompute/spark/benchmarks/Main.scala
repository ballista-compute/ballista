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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.ballistacompute.spark.benchmarks.tpch.Tpch
import org.rogach.scallop.{ScallopConf, Subcommand}

class Conf(args: Array[String]) extends ScallopConf(args) {
  val convertTpch = new Subcommand("convert-tpch") {
    val input = opt[String](required = true)
    val inputFormat = opt[String](required = true)
    val output = opt[String](required = true)
    val outputFormat = opt[String](required = true)
    val partitions = opt[Int](required = true)
  }
  val tpch = new Subcommand("tpch") {
    val inputPath = opt[String]()
    val inputFormat = opt[String]()
    val query = opt[String]()
  }
  addSubcommand(convertTpch)
  addSubcommand(tpch)
  requireSubcommand()
  verify()
}

/**
  * This benchmark is designed to be called as a Docker container.
  */
object Main {

  val tables = Seq(
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region"
  )

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val spark: SparkSession = SparkSession.builder
      .appName("Ballista Spark Benchmarks")
      .master("local[*]")
      .getOrCreate()

    conf.subcommand match {
      case Some(conf.tpch) =>
        for (table <- tables) {
          val df = readTable(conf, spark, table)
          df.createTempView(table)
        }

        val sql = Tpch.query(conf.tpch.query())
        val resultDf = spark.sql(sql)
        resultDf.show()

      case Some(conf.`convertTpch`) =>
        for (table <- tables) {
          val df = readTable(conf, spark, table)

          conf.convertTpch.outputFormat() match {
            case "parquet" =>
              val path = s"${conf.convertTpch.output()}/${table}"
              df.repartition(conf.convertTpch.partitions())
                .write
                .mode(SaveMode.Overwrite)
                .parquet(path)
            case "csv" =>
              val path = s"${conf.convertTpch.output()}/${table}.csv"
              df.repartition(conf.convertTpch.partitions())
                .write
                .mode(SaveMode.Overwrite)
                .csv(path)
            case _ =>
              throw new IllegalArgumentException("unsupported output format")
          }
        }

      case _ =>
        throw new IllegalArgumentException("no subcommand specified")
    }
  }

  private def readTable(
      conf: Conf,
      spark: SparkSession,
      tableName: String
  ): DataFrame = {
    conf.convertTpch.inputFormat() match {
      case "tbl" =>
        val path = s"${conf.convertTpch.input()}/${tableName}.tbl"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "csv" =>
        val path = s"${conf.convertTpch.input()}/${tableName}.csv"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "parquet" =>
        val path = s"${conf.convertTpch.input()}/${tableName}"
        spark.read
          .parquet(path)
      case _ =>
        throw new IllegalArgumentException("unsupported input format")
    }
  }
}
