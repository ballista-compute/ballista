package org.ballistacompute.physical

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType
import java.lang.IllegalStateException
import java.lang.UnsupportedOperationException
import java.util.*
import kotlin.math.ln
import kotlin.math.sqrt

/**
 * Physical representation of an expression.
 */
interface PhysicalExpr {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): ColumnVector
}

class ColumnPExpr(val i: Int) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }
}

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

abstract class ComparisonPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    abstract fun compare(l: ColumnVector, r: ColumnVector) : ColumnVector
}

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

class EqPExpr(l: PhysicalExpr, r: PhysicalExpr) : BinaryPExpr(l, r) {
    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until l.size()).forEach {
            if (l.getValue(it) == r.getValue(it)) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }
        }
        return ArrowFieldVector(v)
    }
}

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

interface AggregatePExpr {
    fun inputExpression(): PhysicalExpr
    fun createAccumulator(): Accumulator
}

class MaxPExpr(private val expr: PhysicalExpr) : AggregatePExpr {

    override fun inputExpression(): PhysicalExpr {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MaxAccumulator()
    }
}


interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}

//class MinAccumulator : Accumulator {I thi
//    override fun accumulate(value: Any?) {
//    }
//}

class MaxAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMax = when (value) {
                    is Int -> value > this.value as Int
                    is Double -> value > this.value as Double
                    is ByteArray -> throw UnsupportedOperationException("MAX is not implemented for String yet")
                    else -> throw UnsupportedOperationException(value.javaClass.name)
                }
                if (isMax) {
                    this.value = value
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }
}
//
//class SumAccumulator : Accumulator {
//    override fun accumulate(value: Any?) {
//    }
//}
//
//class AvgAccumulator : Accumulator {
//    override fun accumulate(value: Any?) {
//    }
//}
//
//class CountAccumulator : Accumulator {
//    override fun accumulate(value: Any?) {
//    }
//}
//
//class CountDistinctAccumulator : Accumulator {
//    override fun accumulate(value: Any?) {
//    }
//}
