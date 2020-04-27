package org.ballistacompute.spark.executor

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location
import org.apache.spark.sql.SparkSession

object SparkExecutor {

  def main(arg: Array[String]): Unit = {
    // https://issues.apache.org/jira/browse/ARROW-5412
    System.setProperty( "io.netty.tryReflectionSetAccessible","true")

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val flightProducer = new SparkFlightProducer(spark)

    val bindHost = "0.0.0.0"
    val port = 50051

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
