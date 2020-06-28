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

import java.lang.IllegalStateException
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.*

class CastExpression(val expr: Expression, val dataType: ArrowType) : Expression {

  override fun toString(): String {
    return "CAST($expr AS $dataType)"
  }

  override fun evaluate(input: RecordBatch): ColumnVector {
    val value: ColumnVector = expr.evaluate(input)
    val fieldVector = FieldVectorFactory.create(dataType, input.rowCount())
    val builder = ArrowVectorBuilder(fieldVector)

    when (dataType) {
      ArrowTypes.Int8Type -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toByte()
                  is String -> vv.toByte()
                  is Number -> vv.toByte()
                  else -> throw IllegalStateException("Cannot cast value to Byte: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.Int16Type -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toShort()
                  is String -> vv.toShort()
                  is Number -> vv.toShort()
                  else -> throw IllegalStateException("Cannot cast value to Short: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.Int32Type -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toInt()
                  is String -> vv.toInt()
                  is Number -> vv.toInt()
                  else -> throw IllegalStateException("Cannot cast value to Int: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.Int64Type -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toLong()
                  is String -> vv.toLong()
                  is Number -> vv.toLong()
                  else -> throw IllegalStateException("Cannot cast value to Long: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.FloatType -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toFloat()
                  is String -> vv.toFloat()
                  is Number -> vv.toFloat()
                  else -> throw IllegalStateException("Cannot cast value to Float: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.DoubleType -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            val castValue =
                when (vv) {
                  is ByteArray -> String(vv).toDouble()
                  is String -> vv.toDouble()
                  is Number -> vv.toDouble()
                  else -> throw IllegalStateException("Cannot cast value to Double: $vv")
                }
            builder.set(it, castValue)
          }
        }
      }
      ArrowTypes.StringType -> {
        (0 until value.size()).forEach {
          val vv = value.getValue(it)
          if (vv == null) {
            builder.set(it, null)
          } else {
            builder.set(it, vv.toString())
          }
        }
      }
      else -> throw IllegalStateException("Cast to $dataType is not supported")
    }

    builder.setValueCount(value.size())
    return builder.build()
  }
}
