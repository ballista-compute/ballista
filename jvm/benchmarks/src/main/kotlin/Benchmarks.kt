package org.ballistacompute.benchmarks

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.execution.ExecutionContext
import kotlin.system.measureTimeMillis

fun main() {

    val path = "/mnt/nyctaxi/"

    sqlAggregate(path)
    sqlAggregate(path)
    sqlAggregate(path)
    sqlAggregate(path)

}

private fun sqlAggregate(path: String) {

    println("BEGIN sqlAggregate")

    val start = System.currentTimeMillis()
    val deferred = (1..12).map { month ->
        GlobalScope.async {

            val sql = "SELECT passenger_count, " +
                    "MIN(CAST(fare_amount AS double)) AS min_fare, MAX(CAST(fare_amount AS double)) AS max_fare, SUM(CAST(fare_amount AS double)) AS sum_fare " +
                    "FROM tripdata " +
                    "GROUP BY passenger_count"

            val start = System.currentTimeMillis()
            val result = executeQuery(path, month, sql)
            val duration = System.currentTimeMillis() - start
            println("Query against month $month took $duration ms")
            result
        }
    }
    val results: List<RecordBatch> = runBlocking {
        deferred.flatMap { it.await() }
    }

    println(results.first().schema)

    val sql = "SELECT passenger_count, " +
            "MIN(max_fare), " +
            "MAX(min_fare), " +
            "SUM(max_fare) " +
            "FROM tripdata " +
            "GROUP BY passenger_count"

    val ctx = ExecutionContext()
    ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
    val df = ctx.sql(sql)
    ctx.execute(df).forEach { println(it) }

    val duration = System.currentTimeMillis() - start
    println("Executed query in $duration ms")

    println("END sqlAggregate")

}

fun executeQuery(path: String, month: Int, sql: String): List<RecordBatch> {
    val monthStr = String.format("%02d", month);
    val filename = "$path/yellow_tripdata_2019-$monthStr.csv"
    val ctx = ExecutionContext()
    ctx.registerCsv("tripdata", filename)
    val df = ctx.sql(sql)
    return ctx.execute(df).toList()
}
