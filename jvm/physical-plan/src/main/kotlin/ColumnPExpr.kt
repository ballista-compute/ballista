package org.ballistacompute.physical

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

/** Reference column in a batch by index */
class ColumnPExpr(val i: Int) : PhysicalExpr {

    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }

    override fun toString(): String {
        return "#$i"
    }
}