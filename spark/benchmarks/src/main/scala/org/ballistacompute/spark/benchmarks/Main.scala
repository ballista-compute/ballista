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

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val spark: SparkSession = SparkSession.builder
      .appName("Ballista Spark Benchmarks")
      .master("local[*]")
      .getOrCreate()

    conf.subcommand match {
      case Some(conf.tpch) =>
        val df = readLineitem(conf, spark)
        df.createTempView("lineitem")
        val sql = Tpch.query(conf.tpch.query())
        val resultDf = spark.sql(sql)
        resultDf.show()

      case Some(conf.`convertTpch`) =>
        val df = readLineitem(conf, spark)

        conf.convertTpch.outputFormat() match {
          case "parquet" =>
            df.repartition(conf.convertTpch.partitions())
              .write
              .mode(SaveMode.Overwrite)
              .parquet(conf.convertTpch.output())
          case "csv" =>
            df.repartition(conf.convertTpch.partitions())
              .write
              .mode(SaveMode.Overwrite)
              .csv(conf.convertTpch.output())
          case _ =>
            throw new IllegalArgumentException("unsupported output format")
        }

      case _ =>
        throw new IllegalArgumentException("no subcommand specified")
    }
  }

  private def readLineitem(conf: Conf, spark: SparkSession): DataFrame = {
    conf.convertTpch.inputFormat() match {
      case "tbl" =>
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .schema(Tpch.LINEITEM_SCHEMA)
          .csv(conf.convertTpch.input())
      case "csv" =>
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .schema(Tpch.LINEITEM_SCHEMA)
          .csv(conf.convertTpch.input())
      case "parquet" =>
        spark.read
          .parquet(conf.convertTpch.input())
      case _ =>
        throw new IllegalArgumentException("unsupported input format")
    }
  }
}
