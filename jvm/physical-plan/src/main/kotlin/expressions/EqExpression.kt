package org.ballistacompute.physical.expressions

import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowTypes
import java.lang.IllegalStateException

class EqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) == (r as Short)
            ArrowTypes.Int32Type -> (l as Int) == (r as Int)
            ArrowTypes.Int64Type -> (l as Long) == (r as Long)
            ArrowTypes.FloatType -> (l as Float) == (r as Float)
            ArrowTypes.DoubleType -> (l as Double) == (r as Double)
            ArrowTypes.StringType -> toString(l) == toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class NeqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) != (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) != (r as Short)
            ArrowTypes.Int32Type -> (l as Int) != (r as Int)
            ArrowTypes.Int64Type -> (l as Long) != (r as Long)
            ArrowTypes.FloatType -> (l as Float) != (r as Float)
            ArrowTypes.DoubleType -> (l as Double) != (r as Double)
            ArrowTypes.StringType -> toString(l) != toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) < (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) < (r as Short)
            ArrowTypes.Int32Type -> (l as Int) < (r as Int)
            ArrowTypes.Int64Type -> (l as Long) < (r as Long)
            ArrowTypes.FloatType -> (l as Float) < (r as Float)
            ArrowTypes.DoubleType -> (l as Double) < (r as Double)
            ArrowTypes.StringType -> toString(l) < toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtEqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) <= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) <= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) <= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) <= (r as Long)
            ArrowTypes.FloatType -> (l as Float) <= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) <= (r as Double)
            ArrowTypes.StringType -> toString(l) <= toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) > (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) > (r as Short)
            ArrowTypes.Int32Type -> (l as Int) > (r as Int)
            ArrowTypes.Int64Type -> (l as Long) > (r as Long)
            ArrowTypes.FloatType -> (l as Float) > (r as Float)
            ArrowTypes.DoubleType -> (l as Double) > (r as Double)
            ArrowTypes.StringType -> toString(l) > toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtEqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) >= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) >= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) >= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) >= (r as Long)
            ArrowTypes.FloatType -> (l as Float) >= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) >= (r as Double)
            ArrowTypes.StringType -> toString(l) >= toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

private fun toString(v: Any?): String{
    return when (v) {
        is ByteArray -> String(v)
        else -> v.toString()
    }
}