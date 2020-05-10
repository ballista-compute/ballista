package org.ballistacompute.examples;

import org.ballistacompute.execution.ExecutionContext
import org.ballistacompute.optimizer.Optimizer

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.logical.*

import kotlin.system.measureTimeMillis


fun main() {

    val ctx = ExecutionContext(mapOf())

    // wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv

    /*
    VendorID: Utf8,
    tpep_pickup_datetime: Utf8,
    tpep_dropoff_datetime: Utf8,
    passenger_count: Utf8,
    trip_distance: Utf8,
    RatecodeID: Utf8,
    store_and_fwd_flag: Utf8,
    PULocationID: Utf8,
    DOLocationID: Utf8,
    payment_type: Utf8,
    fare_amount: Utf8,
    extra: Utf8,
    mta_tax: Utf8,
    tip_amount: Utf8,
    tolls_amount: Utf8,
    improvement_surcharge: Utf8,
    total_amount: Utf8,
    congestion_surcharge: Utf8
    */

    val time = measureTimeMillis {
        val df = ctx.csv("/mnt/nyctaxi/csv/year=2019/yellow_tripdata_2019-01.csv")
                .aggregate(
                        listOf(col("passenger_count")),
                        listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

        println("Logical Plan:\t${format(df.logicalPlan())}")


//        var results = ctx.execute(df.logicalPlan())
//        results.forEach {
//            println(it.schema)
//            println(it.toCSV())
//        }

        val optimizedPlan = Optimizer().optimize(df.logicalPlan())
        println("Optimized Plan:\t${format(optimizedPlan)}")

        val results = ctx.execute(optimizedPlan)
        results.forEach {
            println(it.schema)
            println(it.toCSV())
        }
    }

    println("Query took $time ms")

}
