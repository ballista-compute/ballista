package org.ballistacompute.datatypes

import org.apache.arrow.vector.types.FloatingPointPrecision

object ArrowTypes {
    val BooleanType = org.apache.arrow.vector.types.pojo.ArrowType.Bool()

    val StringType = org.apache.arrow.vector.types.pojo.ArrowType.Utf8()

    val Int8Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(8, true)
    val Int16Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(16, true)
    val Int32Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)
    val Int64Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)

    val UInt8Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(8, false)
    val UInt16Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(16, false)
    val UInt32Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(32, false)
    val UInt64Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(64, false)

    val FloatType = org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    val DoubleType = org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
}