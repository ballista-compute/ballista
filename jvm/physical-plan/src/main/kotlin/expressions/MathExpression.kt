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

abstract class MathExpression(l: Expression, r: Expression) : BinaryExpression(l, r) {

  override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
    val fieldVector = FieldVectorFactory.create(l.getType(), l.size())
    val builder = ArrowVectorBuilder(fieldVector)
    (0 until l.size()).forEach {
      val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
      builder.set(it, value)
    }
    builder.setValueCount(l.size())
    return builder.build()
  }

  abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any?
}

class AddExpression(l: Expression, r: Expression) : MathExpression(l, r) {
  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
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

class SubtractExpression(l: Expression, r: Expression) : MathExpression(l, r) {
  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
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

class MultiplyExpression(l: Expression, r: Expression) : MathExpression(l, r) {
  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
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

class DivideExpression(l: Expression, r: Expression) : MathExpression(l, r) {
  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
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
