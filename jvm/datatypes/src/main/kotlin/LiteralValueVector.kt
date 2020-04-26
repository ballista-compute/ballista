package org.ballistacompute.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType
import java.lang.IllegalStateException
import java.lang.IndexOutOfBoundsException

/** Represents a literal value */
class LiteralValueVector(val arrowType: ArrowType, val value: Any?, val size: Int) : ColumnVector {

    override fun getType(): ArrowType {
        return arrowType
    }

    override fun getValue(i: Int): Any? {
        if (i<0 || i>=size) {
            throw IndexOutOfBoundsException()
        }
        return value
    }

    override fun size(): Int {
        return size
    }

}