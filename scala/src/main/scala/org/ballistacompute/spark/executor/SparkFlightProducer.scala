package org.ballistacompute.spark.executor

import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}

import org.ballistacompute.protobuf

class SparkFlightProducer extends FlightProducer {

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {

    assert(listener != null)
    assert(ticket.getBytes != null)

    val protobufPlan = protobuf.LogicalPlanNode.parseFrom(ticket.getBytes)
    val logicalPlan = new protobuf.ProtobufDeserializer().fromProto(protobufPlan)
    println("Ballista logical plan:\n${logicalPlan.pretty()}")

    val schema = logicalPlan.schema()
    println(schema)

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = ???

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = ???
}
