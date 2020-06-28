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

import java.lang.UnsupportedOperationException

class SumExpression(private val expr: Expression) : AggregateExpression {

  override fun inputExpression(): Expression {
    return expr
  }

  override fun createAccumulator(): Accumulator {
    return SumAccumulator()
  }

  override fun toString(): String {
    return "SUM($expr)"
  }
}

class SumAccumulator : Accumulator {

  var value: Any? = null

  override fun accumulate(value: Any?) {
    if (value != null) {
      if (this.value == null) {
        this.value = value
      } else {
        val currentValue = this.value
        when (currentValue) {
          is Byte -> this.value = currentValue + value as Byte
          is Short -> this.value = currentValue + value as Short
          is Int -> this.value = currentValue + value as Int
          is Long -> this.value = currentValue + value as Long
          is Float -> this.value = currentValue + value as Float
          is Double -> this.value = currentValue + value as Double
          else ->
              throw UnsupportedOperationException(
                  "MIN is not implemented for type: ${value.javaClass.name}")
        }
      }
    }
  }

  override fun finalValue(): Any? {
    return value
  }
}
