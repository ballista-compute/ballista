package org.ballistacompute.datatypes

import org.apache.arrow.vector.types.FloatingPointPrecision

object ArrowTypes {
    val BooleanType = org.apache.arrow.vector.types.pojo.ArrowType.Bool()
    val StringType = org.apache.arrow.vector.types.pojo.ArrowType.Utf8()
    val FloatType = org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    val Int32Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)
    val Int64Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)
    val UInt32Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(32, false)
    val UInt64Type = org.apache.arrow.vector.types.pojo.ArrowType.Int(64, false)
    val DoubleType = org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
}