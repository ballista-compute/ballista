package org.ballistacompute.physical.expressions

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import java.lang.IllegalStateException

abstract class ComparisonExpression(val l: Expression, val r: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        assert(ll.size() == rr.size())
        if (ll.getType() != rr.getType()) {
            throw IllegalStateException("Cannot compare values of different type: ${ll.getType()} != ${rr.getType()}")
        }
        return compare(ll, rr)
    }

    fun compare(l: ColumnVector, r: ColumnVector): ColumnVector {
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until l.size()).forEach {
            if (evaluate(l.getValue(it), r.getValue(it), l.getType())) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean
}