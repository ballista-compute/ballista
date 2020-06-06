// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.physical.expressions

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import java.lang.IllegalStateException

abstract class BooleanExpression(val l: Expression, val r: Expression) : Expression {

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
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            v.set(it, if (value) 1 else 0)
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean
}

class AndExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean {
        return toBool(l) && toBool(r)
    }
}

class OrExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean {
        return toBool(l) || toBool(r)
    }
}

class EqExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

class NeqExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

class LtExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

class LtEqExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

class GtExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

class GtEqExpression(l: Expression, r: Expression): BooleanExpression(l,r) {
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

private fun toBool(v: Any?): Boolean {
    return when (v) {
        is Boolean -> v
        is Number -> v.toInt() == 1
        else -> throw IllegalStateException()
    }
}