package org.ballistacompute.logical

import org.apache.arrow.vector.types.pojo.Schema


/**
 * A logical plan represents a data transformation or action that returns a relation (a set of tuples).
 */
interface LogicalPlan {

    /**
     * Returns the schema of the data that will be produced by this logical plan.
     */
    fun schema(): Schema

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    fun children(): List<LogicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

/** Format a logical plan in human-readable form */
fun format(plan: LogicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent+1)) }
    return b.toString()
}
