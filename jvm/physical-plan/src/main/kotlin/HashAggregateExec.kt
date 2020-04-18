package org.ballistacompute.physical

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ArrowVectorBuilder

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.ballistacompute.datatypes.Schema
import org.ballistacompute.physical.expressions.Accumulator
import org.ballistacompute.physical.expressions.AggregateExpression
import org.ballistacompute.physical.expressions.Expression

class HashAggregateExec(val input: PhysicalPlan,
                        val groupExpr: List<Expression>,
                        val aggregateExpr: List<AggregateExpression>,
                        val schema: Schema) : PhysicalPlan {

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
    }

    override fun execute(): Sequence<RecordBatch> {

        val map = HashMap<List<Any?>, List<Accumulator>>()

        input.execute().iterator().forEach { batch ->

            // evaluate the grouping expressions
            val groupKeys = groupExpr.map { it.evaluate(batch) }

            // evaluate the expressions that are inputs to the aggregate functions
            val aggrInputValues = aggregateExpr.map { it.inputExpression().evaluate(batch) }

            (0 until batch.rowCount()).forEach { rowIndex ->

                val rowKey = groupKeys.map {
                    val value = it.getValue(rowIndex)
                    when (value) {
                        is ByteArray -> String(value)
                        else -> value
                    }
                }

                //println(rowKey)

                // get or create accumulators for this grouping key
                val accumulators = map.getOrPut(rowKey) {
                    aggregateExpr.map { it.createAccumulator() }
                }

                // perform accumulation
                accumulators.withIndex().forEach { accum ->
                    accum.value.accumulate(aggrInputValues[accum.index].getValue(rowIndex))
                }

            }
        }

        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = map.size

        val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

        map.entries.withIndex().forEach { entry ->
            val rowIndex = entry.index
            val groupingKey = entry.value.key
            val accumulators = entry.value.value
            groupExpr.indices.forEach {
                builders[it].set(rowIndex, groupingKey[it])
            }
            aggregateExpr.indices.forEach {
                builders[groupExpr.size+it].set(rowIndex, accumulators[it].finalValue())
            }
        }

        val outputBatch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
        //println("HashAggregateExec output:\n${outputBatch.toCSV()}")
        return listOf(outputBatch).asSequence()
    }

}

