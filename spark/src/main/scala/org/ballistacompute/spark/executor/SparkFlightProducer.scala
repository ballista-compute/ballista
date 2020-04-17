package org.ballistacompute.spark.executor

import scala.collection.JavaConverters._

import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.SparkSession
import org.ballistacompute.protobuf


class SparkFlightProducer(spark: SparkSession) extends FlightProducer {

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    println("getStream()")

    try {
      val action: protobuf.Action = protobuf.Action.parseFrom(ticket.getBytes)

      val logicalPlan = new protobuf.ProtobufDeserializer().fromProto(action.getQuery)

      println(s"Ballista logical plan:\n${logicalPlan.pretty()}")

      val ctx = new BallistaSparkContext(spark)
      val df = ctx.createDataFrame(logicalPlan, None)
      df.explain()

      // collect entire result set into memory - not scalable
      val rows = df.collect()

      val allocator = new RootAllocator(Long.MaxValue)
      val root = VectorSchemaRoot.create(logicalPlan.schema(), allocator)
      root.setRowCount(rows.length)
      root.allocateNew()

      listener.start(root, null)

      rows.foreach { row =>
        //TODO
      }

      listener.completed()

    } catch {
      case e: Exception =>
      e.printStackTrace()
        listener.error(e)
    }

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = ???

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = ???

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = ???

  override def listActions(context: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = ???
}
