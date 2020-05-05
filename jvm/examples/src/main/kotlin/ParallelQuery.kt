package org.ballistacompute.examples

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.execution.ExecutionContext
import kotlin.system.measureTimeMillis

fun main() {

    val path = "/mnt/nyctaxi/csv/yellow/2019"

    val start = System.currentTimeMillis()
    val deferred = (1..2).map {month ->
        GlobalScope.async {

            val sql = "SELECT passenger_count, " +
                    "MAX(CAST(fare_amount AS double)) AS max_fare " +
                    "FROM tripdata " +
                    "GROUP BY passenger_count"

            val partitionStart = System.currentTimeMillis()
            val result = executeQuery(path, month, sql)
            val duration = System.currentTimeMillis() - partitionStart
            println("Query against month $month took $duration ms")
            result
        }
    }
    val results: List<RecordBatch> = runBlocking {
        deferred.flatMap { it.await() }
    }
    val duration = System.currentTimeMillis() - start
    println("Collected ${results.size} batches in $duration ms")

    println(results.first().schema)

    val sql = "SELECT passenger_count, " +
            "MAX(max_fare) " +
            "FROM tripdata " +
            "GROUP BY passenger_count"

    val ctx = ExecutionContext(mapOf())
    ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
    val df = ctx.sql(sql)
    ctx.execute(df).forEach { println(it) }

}

fun executeQuery(path: String, month: Int, sql: String): List<RecordBatch> {
    val monthStr = String.format("%02d", month);
    val filename = "$path/yellow_tripdata_2019-$monthStr.csv"
    val ctx = ExecutionContext(mapOf())
    ctx.registerCsv("tripdata", filename)
    val df = ctx.sql(sql)
    return ctx.execute(df).toList()
}
