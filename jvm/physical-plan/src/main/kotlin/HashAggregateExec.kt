// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

        // for each batch from the input executor
        input.execute().iterator().forEach { batch ->

            // evaluate the grouping expressions
            val groupKeys = groupExpr.map { it.evaluate(batch) }

            // evaluate the expressions that are inputs to the aggregate functions
            val aggrInputValues = aggregateExpr.map { it.inputExpression().evaluate(batch) }

            // for each row in the batch
            (0 until batch.rowCount()).forEach { rowIndex ->

                // create the key for the hash map
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
                    val value = aggrInputValues[accum.index].getValue(rowIndex)
                    accum.value.accumulate(value)
                }

            }
        }

        // create result batch containing final aggregate values
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

