package org.ballistacompute.logical

import org.ballistacompute.datatypes.Schema

/**
 * Logical plan representing a projection (evaluating a list of expressions) against an input
 */
class Projection(val input: LogicalPlan, val expr: List<LogicalExpr>): LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Projection: ${ expr.map { it.toString() }.joinToString(", ") }"
    }
}