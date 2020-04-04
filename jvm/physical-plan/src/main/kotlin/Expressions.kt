package org.ballistacompute.physical

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.LiteralValueVector

/**
 * Physical representation of an expression.
 */
interface PhysicalExpr {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): ColumnVector
}

class LiteralLongPExpr(val value: Long) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralDoublePExpr(val value: Double) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralStringPExpr(val value: String) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value.toByteArray(), input.rowCount())
    }
}

interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}
