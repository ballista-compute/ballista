package org.ballistacompute.spark.executor

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location

import org.ballistacompute.executor.BallistaFlightProducer

object SparkExecutor {

  def main(arg: Array[String]): Unit = {
    // https://issues.apache.org/jira/browse/ARROW-5412
    System.setProperty( "io.netty.tryReflectionSetAccessible","true")

    val server = FlightServer.builder(
      new RootAllocator(Long.MaxValue),
      Location.forGrpcInsecure("localhost", 50051),
      new BallistaFlightProducer())
      .build()
    server.start()

    while (true) {
      Thread.sleep(1000)
    }
  }

}
