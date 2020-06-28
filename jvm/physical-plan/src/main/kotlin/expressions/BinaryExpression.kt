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
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

/**
 * For binary expressions we need to evaluate the left and right input expressions and then evaluate
 * the specific binary operator against those input values, so we can use this base class to
 * simplify the implementation for each operator.
 */
abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    val ll = l.evaluate(input)
    val rr = r.evaluate(input)
    assert(ll.size() == rr.size())
    if (ll.getType() != rr.getType()) {
      throw IllegalStateException(
          "Binary expression operands do not have the same type: " +
              "${ll.getType()} != ${rr.getType()}")
    }
    return evaluate(ll, rr)
  }

  abstract fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector
}
