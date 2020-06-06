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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import kotlin.math.ln
import kotlin.math.sqrt

/** Base class for unary math expressions */
abstract class UnaryMathExpression(private val expr: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val n = expr.evaluate(input);
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until n.size()).forEach {
            val nv = n.getValue(it)
            if (nv == null) {
                v.setNull(it)
            } else if (nv is Double) {
                v.set(it, sqrt(nv))
            } else {
                TODO()
            }
        }
        return ArrowFieldVector(v)
    }

    abstract fun apply(value: Double): Double
}


/** Square root */
class Sqrt(expr: Expression) : UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return sqrt(value)
    }
}

/** Natural logarithm */
class Log(expr: Expression) : UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return ln(value)
    }
}
