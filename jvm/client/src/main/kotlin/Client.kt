package org.ballistacompute.client

import org.ballistacompute.logical.Column as KQColumn
import org.ballistacompute.logical.LogicalExpr
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.logical.Projection as KQProjection
import org.apache.arrow.flight.CallOptions
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.Ticket
import org.apache.arrow.memory.RootAllocator
import org.ballistacompute.protobuf.LogicalPlanNode
import org.ballistacompute.protobuf.ProtoUtils

import java.util.concurrent.TimeUnit

/**
 * Connection to a Ballista executor.
 */
class Client(val host: String, val port: Int) {

    var client = FlightClient.builder()
            .allocator(RootAllocator(Long.MAX_VALUE))
            .location(Location.forGrpcInsecure(host, port))
            .build()

    var callOptions = CallOptions.timeout(600, TimeUnit.SECONDS)

    fun execute(plan: LogicalPlan) {

        val protoBuf = ProtoUtils().toProto(plan)

        var ticket = Ticket(protoBuf.toByteArray())

        var stream = client.getStream(ticket, callOptions)

    }


}