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

package org.ballistacompute.spark.executor

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.SparkSession

object SparkExecutor {

  def main(arg: Array[String]): Unit = {
    val name = SparkExecutor.getClass.getPackage.getImplementationTitle
    val version = SparkExecutor.getClass.getPackage.getImplementationVersion
    println(s"Starting $name $version")

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
