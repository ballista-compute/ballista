package org.ballistacompute.executor

import org.ballistacompute.protobuf.LogicalPlanNode
import org.ballistacompute.protobuf.ProtobufDeserializer
import org.ballistacompute.logical.format

import org.apache.arrow.flight.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.ballistacompute.execution.ExecutionContext

import java.lang.IllegalArgumentException

class BallistaFlightProducer : FlightProducer {

    val ctx = ExecutionContext()

    override fun getStream(context: FlightProducer.CallContext?, ticket: Ticket?, listener: FlightProducer.ServerStreamListener?) {

        if (listener == null) {
            throw IllegalArgumentException()
        }

        val protobufPlan = LogicalPlanNode.parseFrom(ticket?.bytes ?: throw IllegalArgumentException())
        val logicalPlan = ProtobufDeserializer().fromProto(protobufPlan)
        println(format(logicalPlan))

        val results = ctx.execute(logicalPlan)
        results.iterator().forEach {
            val root = VectorSchemaRoot.create(it.schema, RootAllocator(Long.MAX_VALUE))
            listener.start(root)
            listener.putNext()
            println(it)
        }
        listener.completed()
    }

    override fun listFlights(context: FlightProducer.CallContext?, criteria: Criteria?, listener: FlightProducer.StreamListener<FlightInfo>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getFlightInfo(context: FlightProducer.CallContext?, descriptor: FlightDescriptor?): FlightInfo {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun listActions(context: FlightProducer.CallContext?, listener: FlightProducer.StreamListener<ActionType>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun acceptPut(context: FlightProducer.CallContext?, flightStream: FlightStream?, ackStream: FlightProducer.StreamListener<PutResult>?): Runnable {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun doAction(context: FlightProducer.CallContext?, action: Action?, listener: FlightProducer.StreamListener<Result>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}