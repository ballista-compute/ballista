package org.ballistacompute.physical

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

abstract class ComparisonPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    abstract fun compare(l: ColumnVector, r: ColumnVector) : ColumnVector
}