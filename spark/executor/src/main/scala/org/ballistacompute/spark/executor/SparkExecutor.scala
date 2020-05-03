package org.ballistacompute.spark.executor

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

object SparkExecutor {

  def main(arg: Array[String]): Unit = {

    //TODO command-line params
    val master = "local[1]" // single-threaded for benchmarks against Rust/JVM executors
    val bindHost = "0.0.0.0"
    val port = 50051

    // https://issues.apache.org/jira/browse/ARROW-5412
    System.setProperty( "io.netty.tryReflectionSetAccessible","true")

    val spark = SparkSession.builder()
      .master(master)
      .getOrCreate()

    val flightProducer = new SparkFlightProducer(spark)

    // start Flight server
    val server = FlightServer.builder(
      new RootAllocator(Long.MaxValue),
      Location.forGrpcInsecure(bindHost, port),
      flightProducer)
      .build()

    server.start()

    println(s"Ballista Spark Executor listening on $bindHost:$port")

    while (true) {
      Thread.sleep(1000)
    }
  }

}
