package org.ballistacompute.optimizer

import org.ballistacompute.logical.*

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
        else -> TODO(expr.javaClass.name)
    }
}

