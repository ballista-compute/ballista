package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import java.lang.IllegalStateException
import kotlin.math.ln
import kotlin.math.sqrt

/** Base class for unary math expressions */
abstract class UnaryMathExpr(private val expr: PhysicalExpr) : PhysicalExpr {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val n = expr.evaluate(input);
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until n.size()).forEach {
            val nv = n.getValue(it)
            if (nv == null) {
                v.setNull(it)
            } else if (nv is Double) {
                v.set(it, sqrt(nv))
            } else {
                throw IllegalStateException()
            }
        }
        return ArrowFieldVector(v)
    }

    abstract fun apply(value: Double): Double
}


/** Square root */
class Sqrt(expr: PhysicalExpr) : UnaryMathExpr(expr) {
    override fun apply(value: Double): Double {
        return sqrt(value)
    }
}

/** Natural logarithm */
class Log(expr: PhysicalExpr) : UnaryMathExpr(expr) {
    override fun apply(value: Double): Double {
        return ln(value)
    }
}
