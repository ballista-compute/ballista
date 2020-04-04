package org.ballistacompute.physical

import org.apache.arrow.vector.types.pojo.Schema
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder
import org.ballistacompute.logical.LogicalPlan


/**
 * A physical plan represents an executable piece of code that will produce data.
 */
interface PhysicalPlan {

    fun schema(): Schema

    /**
     * Execute a physical plan and produce a series of record batches.
     */
    fun execute(): Sequence<RecordBatch>

    /**
     * Returns the children (inputs) of this physical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    fun children(): List<PhysicalPlan>

    fun pretty(): String {
        return format(this)
    }

}

/** Format a logical plan in human-readable form */
private fun format(plan: PhysicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent+1)) }
    return b.toString()
}




