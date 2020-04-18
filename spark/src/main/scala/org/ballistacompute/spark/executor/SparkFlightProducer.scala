package org.ballistacompute.spark.executor

import java.util
import java.util.{ArrayList, List}

import scala.collection.JavaConverters._
import io.netty.buffer.ArrowBuf

import scala.collection.JavaConverters._
import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{FieldVector, Float4Vector, Float8Vector, IntVector, TypeLayout, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot}
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
      val sparkSchema = df.schema

      val allocator = new RootAllocator(Long.MaxValue)
      val root = VectorSchemaRoot.create(logicalPlan.schema().toArrow(), allocator)
      root.allocateNew()

      listener.start(root, null)

      rows.zipWithIndex.foreach {

        //TODO null handling

        case (row, row_index) =>
          for (i <- 0 until sparkSchema.length) {
            root.getVector(i) match {
              case v: IntVector =>
                if (row.isNullAt(i)) {
                  v.setNull(row_index)
                } else {
                  v.set(row_index, row.getInt(i))
                }
              case v: Float4Vector =>
                if (row.isNullAt(i)) {
                  v.setNull(row_index)
                } else {
                  v.set(row_index, row.getFloat(i))
                }
              case v: Float8Vector =>
                if (row.isNullAt(i)) {
                  v.setNull(row_index)
                } else {
                  v.set(row_index, row.getDouble(i))
                }
              case v: VarCharVector =>
                if (row.isNullAt(i)) {
                  v.setNull(row_index)
                } else {
                  v.set(row_index, row.getString(i).getBytes)
                }
              case other =>
                println(s"No support for $other yet")
            }
          }
      }

      root.setRowCount(rows.length)
      listener.putNext()

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

  def getRecordBatch(root: VectorSchemaRoot): ArrowRecordBatch = {
    val nodes: util.List[ArrowFieldNode] = new util.ArrayList[ArrowFieldNode]
    val buffers: util.List[ArrowBuf] = new util.ArrayList[ArrowBuf]
    for (vector <- root.getFieldVectors.asScala) {
      appendNodes(vector, nodes, buffers)
    }
    new ArrowRecordBatch(root.getRowCount, nodes, buffers, true)
  }

  private def appendNodes(vector: FieldVector, nodes: util.List[ArrowFieldNode], buffers: util.List[ArrowBuf]): Unit = {
    val includeNullCount = true
    nodes.add(new ArrowFieldNode(vector.getValueCount, if (includeNullCount) vector.getNullCount else -1))
    val fieldBuffers: util.List[ArrowBuf] = vector.getFieldBuffers
    val expectedBufferCount: Int = TypeLayout.getTypeBufferCount(vector.getField.getType)
    if (fieldBuffers.size != expectedBufferCount) throw new IllegalArgumentException(String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField, vector.getClass.getSimpleName, fieldBuffers))
    buffers.addAll(fieldBuffers)
    for (child <- vector.getChildrenFromFields.asScala) {
      appendNodes(child, nodes, buffers)
    }
  }
}
