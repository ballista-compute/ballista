package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ColumnVector
import java.util.*

class EqExpr(l: PhysicalExpr, r: PhysicalExpr): ComparisonPExpr(l,r) {

    override fun compare(l: ColumnVector, r: ColumnVector): ColumnVector {
        assert(l.size() == r.size())
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        (0 until l.size()).forEach {
            if (eq(l.getValue(it), r.getValue(it))) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    private fun eq(l: Any?, r: Any?) : Boolean {
        //TODO
        return if (l is ByteArray) {
            if (r is ByteArray) {
                Arrays.equals(l, r)
            } else if (r is String) {
                Arrays.equals(l, r.toByteArray())
            } else {
                TODO()
            }
        } else {
            l == r
        }
    }
}