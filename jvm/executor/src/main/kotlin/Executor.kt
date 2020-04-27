package org.ballistacompute.executor

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location

class Executor {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            // https://issues.apache.org/jira/browse/ARROW-5412
            System.setProperty( "io.netty.tryReflectionSetAccessible","true")

            val bindHost = "0.0.0.0"
            val port = 50051

            val server = FlightServer.builder(
                    RootAllocator(Long.MAX_VALUE),
                    Location.forGrpcInsecure(bindHost, port),
                    BallistaFlightProducer())
                    .build()
            server.start()

            println("Listening on $bindHost:$port")

            while (true) {
                Thread.sleep(1000)
            }
        }
    }
}