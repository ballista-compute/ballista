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

import scala.collection.mutable.ListBuffer

class Conf(args: Array[String]) extends ScallopConf(args) {
  val convertTpch = new Subcommand("convert-tpch") {
    val input = opt[String](required = true)
    val inputFormat = opt[String](required = true)
    val output = opt[String](required = true)
    val outputFormat = opt[String](required = true)
    val partitions = opt[Int](required = true)
  }
  val tpch = new Subcommand("tpch") {
    val inputPath = opt[String](required = true)
    val inputFormat = opt[String](required = true)
    val query = opt[String](required = true)
    val iterations = opt[Int](required = false, default = Some(1))
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
        // register tables
        for (table <- Tpch.tables) {
          val df = readTable(
            conf,
            spark,
            table,
            conf.tpch.inputPath(),
            conf.tpch.inputFormat()
          )
          df.createTempView(table)
        }

        val sql = Tpch.query(conf.tpch.query())

        val timing = new ListBuffer[Long]()
        for (i <- 0 until conf.tpch.iterations()) {
          println(s"Iteration $i")
          val start = System.currentTimeMillis()
          val resultDf = spark.sql(sql)
          resultDf.show()
          val duration = System.currentTimeMillis() - start
          println(s"Iteration $i took $duration ms")
          timing += duration
        }

        // summarize the results
        timing.zipWithIndex.foreach {
          case (n, i) => println(s"Iteration $i took $n ms")
        }

      case Some(conf.`convertTpch`) =>
        for (table <- Tpch.tables) {
          val df = readTable(
            conf,
            spark,
            table,
            conf.convertTpch.input(),
            conf.convertTpch.inputFormat()
          )

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
      tableName: String,
      inputPath: String,
      inputFormat: String
  ): DataFrame = {
    inputFormat match {
      case "tbl" =>
        val path = s"${inputPath}/${tableName}.tbl"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "csv" =>
        val path = s"${inputPath}/${tableName}.csv"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "parquet" =>
        val path = s"${inputPath}/${tableName}"
        spark.read
          .parquet(path)
      case _ =>
        throw new IllegalArgumentException("unsupported input format")
    }
  }
}
