package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ArrowVectorBuilder
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

class CastPExpr(val expr: PhysicalExpr, val dataType: ArrowType) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val value = expr.evaluate(input)
        return when (dataType) {
            is ArrowType.Int -> {
                //TODO move this logic to separate source file
                val v = IntVector("v", RootAllocator(Long.MAX_VALUE))
                v.allocateNew()

                val builder = ArrowVectorBuilder(v)
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        when (vv) {
                            is ByteArray -> builder.set(it, String(vv).toInt())
                            else -> TODO()
                        }
                    }
                }
                v.valueCount = value.size()
                ArrowFieldVector(v)
            }
            is ArrowType.FloatingPoint -> {
                //TODO move this logic to separate source file
                val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
                v.allocateNew()

                val builder = ArrowVectorBuilder(v)
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        when (vv) {
                            is ByteArray -> builder.set(it, String(vv).toDouble())
                            else -> TODO()
                        }
                    }
                }
                v.valueCount = value.size()
                ArrowFieldVector(v)
            }
            else -> TODO()
        }
    }
}