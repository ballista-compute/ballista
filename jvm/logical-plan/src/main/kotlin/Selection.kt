package org.ballistacompute.logical

import org.ballistacompute.datatypes.Schema

/**
 * Logical plan representing a selection (a.k.a. filter) against an input
 */
class Selection(val input: LogicalPlan, val expr: LogicalExpr): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        // selection does not change the schema of the input
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}