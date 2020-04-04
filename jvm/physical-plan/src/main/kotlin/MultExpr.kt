package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.ballistacompute.datatypes.ColumnVector

class MultExpr(l: PhysicalExpr, r: PhysicalExpr): BinaryPExpr(l,r) {

    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {

        assert(l.size() == r.size())
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        //TODO make this generic so it supports all numeric types .. this is hard coded for the one test that uses it

        TODO()

//        when (l) {
//            is BigIntVector -> {
//                val rr = r as Float8Vector
//                (0 until l.valueCount).forEach {
//                    val leftValue = l.get(it)
//                    val rightValue = rr.get(it)
//                    ////println("${String(leftValue)} == ${String(rightValue)} ?")
//                    v.set(it, leftValue.toDouble() * rightValue)
//                }
//            }
//            else -> TODO()
//        }
//        v.valueCount = l.valueCount
//        return ArrowFieldVector(v)
    }
}