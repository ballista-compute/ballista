package org.ballistacompute.datatypes

import org.apache.arrow.vector.*
import java.lang.IllegalStateException

class ArrowVectorBuilder(val fieldVector: FieldVector) {

    fun set(i: Int, value: Any?) {
        when (fieldVector) {
            is VarCharVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is ByteArray) {
                    fieldVector.set(i, value)
                } else {
                    fieldVector.set(i, value.toString().toByteArray())
                }
            }
            is TinyIntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toByte())
                } else if (value is String) {
                    fieldVector.set(i, value.toByte())
                } else {
                    throw IllegalStateException()
                }
            }
            is SmallIntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toShort())
                } else if (value is String) {
                    fieldVector.set(i, value.toShort())
                } else {
                    throw IllegalStateException()
                }
            }
            is IntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toInt())
                } else if (value is String) {
                    fieldVector.set(i, value.toInt())
                } else {
                    throw IllegalStateException()
                }
            }
            is BigIntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toLong())
                } else if (value is String) {
                    fieldVector.set(i, value.toLong())
                } else {
                    throw IllegalStateException()
                }
            }
            is Float4Vector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toFloat())
                } else if (value is String) {
                    fieldVector.set(i, value.toFloat())
                } else {
                    throw IllegalStateException()
                }
            }
            is Float8Vector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toDouble())
                } else if (value is String) {
                    fieldVector.set(i, value.toDouble())
                } else {
                    throw IllegalStateException()
                }
            }
            else -> throw IllegalStateException(fieldVector.javaClass.name)
        }
    }

    fun setValueCount(n: Int) {
        fieldVector.valueCount = n
    }

    fun build(): ColumnVector {
        return ArrowFieldVector(fieldVector)
    }

}