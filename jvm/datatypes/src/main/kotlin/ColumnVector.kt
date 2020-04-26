package org.ballistacompute.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * Abstraction over different implementations of a column vector.
 */
interface ColumnVector {
    fun getType(): ArrowType
    fun getValue(i: Int) : Any?
    fun size(): Int
}



