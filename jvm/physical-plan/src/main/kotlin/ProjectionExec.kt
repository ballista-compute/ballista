package org.ballistacompute.physical

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema

import org.ballistacompute.physical.expressions.Expression

/**
 * Execute a projection.
 */
class ProjectionExec(val input: PhysicalPlan,
                     val schema: Schema,
                     val expr: List<Expression>) : PhysicalPlan {

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun execute(): Sequence<RecordBatch> {
        return input.execute().map { batch ->
            val columns = expr.map { it.evaluate(batch) }
            RecordBatch(schema, columns)
        }
    }

    override fun toString(): String {
        return "ProjectionExec: $expr"
    }
}