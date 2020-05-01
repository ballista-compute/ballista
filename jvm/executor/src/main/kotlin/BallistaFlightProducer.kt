package org.ballistacompute.executor

import org.apache.arrow.flight.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.ballistacompute.execution.ExecutionContext
import org.ballistacompute.logical.format
import java.lang.IllegalStateException

class BallistaFlightProducer : FlightProducer {

    val ctx = ExecutionContext()

    override fun getStream(context: FlightProducer.CallContext?, ticket: Ticket?, listener: FlightProducer.ServerStreamListener?) {

        if (listener == null) {
            throw IllegalArgumentException()
        }

        try {

            val action = org.ballistacompute.protobuf.Action.parseFrom(ticket?.bytes ?: throw IllegalArgumentException())
            val logicalPlan = org.ballistacompute.protobuf.ProtobufDeserializer().fromProto(action.query)
            println(logicalPlan.pretty())

            val schema = logicalPlan.schema()
            println(schema)

            val allocator = RootAllocator(Long.MAX_VALUE)
            val root = VectorSchemaRoot.create(schema.toArrow(), allocator)
            root.fieldVectors.forEach { it.setInitialCapacity(64*1024) }
            root.allocateNew()

            val results = ctx.execute(logicalPlan)
            listener.start(root, null)

            //val loader = VectorLoader(root)
            var counter = 0
            results.iterator().forEach { batch ->

                val rowCount = batch.rowCount()
                println("Received batch with $rowCount rows")

                (0 until schema.fields.size).forEach { columnIndex ->

                    val sourceVector = batch.fields[columnIndex]
                    val v = root.fieldVectors[columnIndex]

                    //TODO this is brute force copying that can be optimized if the underlying data is already in Arrow format
                    when (v) {
                        is TinyIntVector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Byte)
                                }
                            }
                        }
                        is SmallIntVector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Short)
                                }
                            }
                        }
                        is IntVector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Int)
                                }
                            }
                        }
                        is BigIntVector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Long)
                                }
                            }
                        }
                        is Float4Vector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Float)
                                }
                            }
                        }
                        is Float8Vector -> {
                            (0 until rowCount).forEach { rowIndex ->
                                val value = sourceVector.getValue(rowIndex)
                                if (value == null) {
                                    v.setNull(rowIndex)
                                } else {
                                    v.set(rowIndex, value as Double)
                                }
                            }
                        }
                        is VarCharVector -> {
                            (0 until rowCount).forEach { ri ->
                                val value = sourceVector.getValue(ri)
                                if (value == null) {
                                    v.setNull(ri)
                                } else {
                                    v.set(ri, value as ByteArray)
                                }
                            }
                        }
                        else -> throw IllegalStateException(v.javaClass.name)
                    }

                }

                root.rowCount = rowCount
                listener.putNext()
                counter++
            }
            listener.completed()
        } catch (ex: Exception) {
            ex.printStackTrace()
            listener.error(ex)
        }
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