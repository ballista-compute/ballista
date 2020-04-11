package org.ballistacompute.executor

import org.apache.arrow.flight.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.ballistacompute.execution.ExecutionContext
import org.ballistacompute.logical.format

class BallistaFlightProducer : FlightProducer {

    val ctx = ExecutionContext()

    override fun getStream(context: FlightProducer.CallContext?, ticket: Ticket?, listener: FlightProducer.ServerStreamListener?) {

        if (listener == null) {
            throw IllegalArgumentException()
        }

//        ctx.register("employee", )

        try {

            val action = org.ballistacompute.protobuf.Action.parseFrom(ticket?.bytes ?: throw IllegalArgumentException())
            val tables = mutableMapOf<String,String>()
            action.tableMetaList.forEach {
                tables.put(it.tableName, it.filename)
            }

            val logicalPlan = org.ballistacompute.protobuf.ProtobufDeserializer(tables).fromProto(action.query)
            println(format(logicalPlan))

            val schema = logicalPlan.schema()
            println(schema)

            val allocator = RootAllocator(Long.MAX_VALUE)
            val root = VectorSchemaRoot.create(schema, allocator)

            val results = ctx.execute(logicalPlan)
            listener.start(root, null)

            //val loader = VectorLoader(root)
            var counter = 0
            results.iterator().forEach { batch ->

                val rowCount = batch.rowCount()
                println("Received batch with $rowCount rows")

                root.allocateNew()
                root.rowCount = rowCount

                (0 until schema.fields.size).forEach { columnIndex ->

                    val sourceVector = batch.fields[columnIndex]
                    val v = root.fieldVectors[columnIndex]

                    //TODO this is brute force copying that can be optimized if the underlying data is already in Arrow format

                    when (v) {
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
                        else -> TODO()
                    }

                }

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