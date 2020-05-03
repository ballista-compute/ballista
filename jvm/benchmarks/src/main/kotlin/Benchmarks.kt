package org.ballistacompute.benchmarks

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.execution.ExecutionContext
import java.io.File
import java.io.FileWriter

/**
 * Designed to be run from Docker. See top-level benchmarks folder for more info.
 */
class Benchmarks {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
//    val sql = System.getenv("BENCH_SQL_PARTIAL")
//    val sql = System.getenv("BENCH_SQL_FINAL")

            //TODO parameterize

            val sqlPartial = "SELECT passenger_count, " +
                    "MIN(CAST(fare_amount AS double)) AS min_fare, MAX(CAST(fare_amount AS double)) AS max_fare, SUM(CAST(fare_amount AS double)) AS sum_fare " +
                    "FROM tripdata " +
                    "GROUP BY passenger_count"

            val sqlFinal = "SELECT passenger_count, " +
                    "MIN(max_fare), " +
                    "MAX(min_fare), " +
                    "SUM(max_fare) " +
                    "FROM tripdata " +
                    "GROUP BY passenger_count"


            val path = System.getenv("BENCH_PATH")
            val resultFile = System.getenv("BENCH_RESULT_FILE")

            //TODO iterations

            sqlAggregate(path, sqlPartial, sqlFinal, resultFile)
        }
    }
}
private fun getFiles(path: String): List<String> {
    //TODO improve to do recursion
    val dir = File(path)
    return dir.list().filter { it.endsWith(".csv") }
}

private fun sqlAggregate(path: String, sqlPartial: String, sqlFinal: String, resultFile: String) {
    val start = System.currentTimeMillis()
    val files = getFiles(path)
    val deferred = files.map { file ->
        GlobalScope.async {
            println("Executing query against $file ...")
            val start = System.currentTimeMillis()
            val result = executeQuery(File(File(path), file).absolutePath, sqlPartial)
            val duration = System.currentTimeMillis() - start
            println("Query against $file took $duration ms")
            result
        }
    }
    val results: List<RecordBatch> = runBlocking {
        deferred.flatMap { it.await() }
    }

    println(results.first().schema)

    val ctx = ExecutionContext()
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

fun executeQuery(path: String, sql: String): List<RecordBatch> {
    val ctx = ExecutionContext()
    ctx.registerCsv("tripdata", path)
    val df = ctx.sql(sql)
    return ctx.execute(df).toList()
}
