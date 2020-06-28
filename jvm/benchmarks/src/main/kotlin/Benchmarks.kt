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

package org.ballistacompute.benchmarks

import java.io.File
import java.io.FileWriter
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.execution.ExecutionContext

/** Designed to be run from Docker. See top-level benchmarks folder for more info. */
class Benchmarks {
  companion object {
    @JvmStatic
    fun main(args: Array<String>) {

      println("maxMemory=${Runtime.getRuntime().maxMemory()}")
      println("totalMemory=${Runtime.getRuntime().totalMemory()}")
      println("freeMemory=${Runtime.getRuntime().freeMemory()}")

      //    val sql = System.getenv("BENCH_SQL_PARTIAL")
      //    val sql = System.getenv("BENCH_SQL_FINAL")

      // TODO parameterize

      val sqlPartial =
          "SELECT passenger_count, " +
              "MIN(CAST(fare_amount AS double)) AS min_fare, MAX(CAST(fare_amount AS double)) AS max_fare, SUM(CAST(fare_amount AS double)) AS sum_fare " +
              "FROM tripdata " +
              "GROUP BY passenger_count"

      val sqlFinal =
          "SELECT passenger_count, " +
              "MIN(max_fare), " +
              "MAX(min_fare), " +
              "SUM(max_fare) " +
              "FROM tripdata " +
              "GROUP BY passenger_count"

      val path = System.getenv("BENCH_PATH")
      val resultFile = System.getenv("BENCH_RESULT_FILE")

      val settings = mapOf(Pair("ballista.csv.batchSize", "1024"))

      // TODO iterations

      sqlAggregate(path, sqlPartial, sqlFinal, resultFile, settings)

      println("maxMemory=${Runtime.getRuntime().maxMemory()}")
      println("totalMemory=${Runtime.getRuntime().totalMemory()}")
      println("freeMemory=${Runtime.getRuntime().freeMemory()}")
    }
  }
}

private fun getFiles(path: String): List<String> {
  // TODO improve to do recursion
  val dir = File(path)
  return dir.list().filter { it.endsWith(".csv") }
}

private fun sqlAggregate(
    path: String,
    sqlPartial: String,
    sqlFinal: String,
    resultFile: String,
    settings: Map<String, String>
) {
  val start = System.currentTimeMillis()
  val files = getFiles(path)
  val deferred =
      files.map { file ->
        GlobalScope.async {
          println("Executing query against $file ...")
          val partitionStart = System.currentTimeMillis()
          val result = executeQuery(File(File(path), file).absolutePath, sqlPartial, settings)
          val duration = System.currentTimeMillis() - partitionStart
          println("Query against $file took $duration ms")
          result
        }
      }
  val results: List<RecordBatch> = runBlocking { deferred.flatMap { it.await() } }

  println(results.first().schema)

  val ctx = ExecutionContext(settings)
  ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
  val df = ctx.sql(sqlFinal)
  ctx.execute(df).forEach { println(it) }

  val duration = System.currentTimeMillis() - start
  println("Executed query in $duration ms")

  val w = FileWriter(File(resultFile))
  w.write("iterations,time_millis\n")
  w.write("1,$duration\n")
  w.close()
}

fun executeQuery(path: String, sql: String, settings: Map<String, String>): List<RecordBatch> {
  val ctx = ExecutionContext(settings)
  ctx.registerCsv("tripdata", path)
  val df = ctx.sql(sql)
  return ctx.execute(df).toList()
}
