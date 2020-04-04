package org.ballistacompute.examples

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.ballistacompute.execution.ExecutionContext

fun main() {

    val deferred = (1..12).map {month ->
        GlobalScope.async {
            val filename = "/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-03.csv" //TODO month
            println(filename)
            val ctx = ExecutionContext()
            ctx.registerCsv("tripdata", filename)
            val df = ctx.sql("SELECT passenger_count, MAX(fare_amount) FROM tripdata GROUP BY passenger_count")
            val results = ctx.execute(df)
            results.forEach { println(it) }
        }
    }
    runBlocking {
        val results = deferred.map { it.await() }
        println(results)
    }
}
