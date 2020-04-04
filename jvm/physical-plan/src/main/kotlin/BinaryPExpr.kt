package org.ballistacompute.physical

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

/**
 * For binary expressions we need to evaluate the left and right input expressions and then evaluate the
 * specific binary operator against those input values, so we can use this base class to simplify the
 * implementation for each operator.
 */
abstract class BinaryPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}