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

package org.ballistacompute.optimizer

import org.ballistacompute.logical.*
import java.lang.IllegalStateException

class Optimizer() {

    fun optimize(plan: LogicalPlan) : LogicalPlan {
        // note there is only one rule implemented so far but later there will be a list
        val rule = ProjectionPushDownRule()
        return rule.optimize(plan)
    }

}

interface OptimizerRule {
    fun optimize(plan: LogicalPlan) : LogicalPlan
}

fun extractColumns(expr: List<LogicalExpr>, input: LogicalPlan, accum: MutableSet<String>) {
    expr.forEach { extractColumns(it, input, accum) }
}

fun extractColumns(expr: LogicalExpr, input: LogicalPlan, accum: MutableSet<String>) {
    when (expr) {
        is ColumnIndex -> accum.add(input.schema().fields[expr.i].name)
        is Column -> accum.add(expr.name)
        is BinaryExpr -> {
            extractColumns(expr.l, input, accum)
            extractColumns(expr.r, input, accum)
        }
        is Alias -> extractColumns(expr.expr, input, accum)
        is CastExpr -> extractColumns(expr.expr, input, accum)
        is LiteralString -> {}
        is LiteralLong -> {}
        is LiteralDouble -> {}
        else -> throw IllegalStateException("extractColumns does not support expression: $expr")
    }
}

