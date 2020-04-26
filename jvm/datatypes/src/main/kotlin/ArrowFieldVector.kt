package org.ballistacompute.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType


/** Wrapper around Arrow FieldVector */
class ArrowFieldVector(val field: FieldVector) : ColumnVector {

    override fun getType(): ArrowType {
        return when (field) {
            is BitVector -> ArrowTypes.BooleanType
            is TinyIntVector -> ArrowTypes.Int8Type
            is SmallIntVector -> ArrowTypes.Int16Type
            is IntVector -> ArrowTypes.Int32Type
            is BigIntVector -> ArrowTypes.Int64Type
            is Float4Vector -> ArrowTypes.FloatType
            is Float8Vector -> ArrowTypes.DoubleType
            is VarCharVector -> ArrowTypes.StringType
            else -> TODO(field.javaClass.name)
        }
    }

    override fun getValue(i: Int) : Any? {

        if (field.isNull(i)) {
            return null
        }

        return when (field) {
            is BitVector -> field.get(i)
            is TinyIntVector -> field.get(i)
            is SmallIntVector -> field.get(i)
            is IntVector -> field.get(i)
            is BigIntVector -> field.get(i)
            is Float4Vector -> field.get(i)
            is Float8Vector -> field.get(i)
            is VarCharVector -> field.get(i)
            else -> TODO(field.javaClass.name)
        }
    }

    override fun size(): Int {
        return field.valueCount
    }

    override fun add(rhs: ColumnVector): ColumnVector {
        when (field) {
            is IntVector -> {
                val fieldVector = IntVector("", RootAllocator(Long.MAX_VALUE))
                fieldVector.allocateNew(field.valueCount)
                fieldVector.valueCount = field.valueCount
                val b = ArrowVectorBuilder(fieldVector)
                (0 until field.valueCount).forEach {
                    b.set(it, field.get(it) + rhs.getValue(it) as Int)
                }
                return b.build()
            }
            else -> TODO()
        }
    }

    override fun subtract(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun multiply(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun divide(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun eq(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun neq(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lt(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lteq(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun gt(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun gteq(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
