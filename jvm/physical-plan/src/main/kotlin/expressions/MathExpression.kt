package org.ballistacompute.physical.expressions

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.*
import java.lang.IllegalStateException

abstract class MathExpression(l: Expression, r: Expression): BinaryExpression(l,r) {

    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
        val fieldVector = FieldVectorFactory.create(l.getType())
        val builder = ArrowVectorBuilder(fieldVector)
        (0 until l.size()).forEach {
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            builder.set(it, value)
        }
        builder.setValueCount(l.size())
        return builder.build()
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any?
}

class AddExpression(l: Expression, r: Expression): MathExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) + (r as Short)
            ArrowTypes.Int32Type -> (l as Int) + (r as Int)
            ArrowTypes.Int64Type -> (l as Long) + (r as Long)
            ArrowTypes.FloatType -> (l as Float) + (r as Float)
            ArrowTypes.DoubleType -> (l as Double) + (r as Double)
            else -> throw IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l+$r"
    }
}

class SubtractExpression(l: Expression, r: Expression): MathExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) - (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) - (r as Short)
            ArrowTypes.Int32Type -> (l as Int) - (r as Int)
            ArrowTypes.Int64Type -> (l as Long) - (r as Long)
            ArrowTypes.FloatType -> (l as Float) - (r as Float)
            ArrowTypes.DoubleType -> (l as Double) - (r as Double)
            else -> throw IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l-$r"
    }
}

class MultiplyExpression(l: Expression, r: Expression): MathExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) * (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) * (r as Short)
            ArrowTypes.Int32Type -> (l as Int) * (r as Int)
            ArrowTypes.Int64Type -> (l as Long) * (r as Long)
            ArrowTypes.FloatType -> (l as Float) * (r as Float)
            ArrowTypes.DoubleType -> (l as Double) * (r as Double)
            else -> throw IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l*$r"
    }
}

class DivideExpression(l: Expression, r: Expression): MathExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) / (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) / (r as Short)
            ArrowTypes.Int32Type -> (l as Int) / (r as Int)
            ArrowTypes.Int64Type -> (l as Long) / (r as Long)
            ArrowTypes.FloatType -> (l as Float) / (r as Float)
            ArrowTypes.DoubleType -> (l as Double) / (r as Double)
            else -> throw IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l/$r"
    }
}