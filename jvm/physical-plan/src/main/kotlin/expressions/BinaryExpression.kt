package org.ballistacompute.physical.expressions

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import java.lang.IllegalStateException

/**
 * For binary expressions we need to evaluate the left and right input expressions and then evaluate the
 * specific binary operator against those input values, so we can use this base class to simplify the
 * implementation for each operator.
 */
abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        assert(ll.size() == rr.size())
        if (ll.getType() != rr.getType()) {
            throw IllegalStateException(
                    "Binary expression operands do not have the same type: " +
                            "${ll.getType()} != ${rr.getType()}")
        }
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}