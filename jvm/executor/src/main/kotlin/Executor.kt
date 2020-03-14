package org.ballistacompute.executor

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location

class Executor {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val server = FlightServer.builder(
                    RootAllocator(Long.MAX_VALUE),
                    Location.forGrpcInsecure("localhost", 50051),
                    BallistaFlightProducer())
                    .build()
            server.start()

            while (true) {
                Thread.sleep(1000)
            }
        }
    }
}