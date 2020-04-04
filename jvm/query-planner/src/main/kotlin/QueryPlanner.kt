package org.ballistacompute.planner

import org.ballistacompute.logical.*
import org.ballistacompute.physical.*
import java.sql.SQLException

/**
 * The query planner creates a physical query plan from a logical query plan.
 */
class QueryPlanner {

    /**
     * Create a physical plan from a logical plan.
     */
    fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
        return when (plan) {
            is Scan -> {
                ScanExec(plan.dataSource, plan.projection)
            }
            is Selection -> {
                val input = createPhysicalPlan(plan.input)
                val filterExpr = createPhysicalExpr(plan.expr, plan.input)
                SelectionExec(input, filterExpr)
            }
            is Projection -> {
                val input = createPhysicalPlan(plan.input)
                val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
                ProjectionExec(input, plan.schema(), projectionExpr)
            }
            is Aggregate -> {
                val input = createPhysicalPlan(plan.input)
                val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
                val aggregateExpr = plan.aggregateExpr.map {
                    when (it) {
                        is Max -> MaxPExpr(createPhysicalExpr(it.expr, plan.input))
                        else -> TODO()
                    }
                }
                HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
            }
            else -> throw IllegalStateException(plan.javaClass.toString())
        }
    }

    /**
     * Create a physical expression from a logical expression.
     */
    fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): PhysicalExpr = when (expr) {
        is LiteralLong -> LiteralLongPExpr(expr.n)
        is LiteralDouble -> LiteralDoublePExpr(expr.n)
        is LiteralString -> LiteralStringPExpr(expr.str)
        is ColumnIndex -> ColumnPExpr(expr.i)
        is Alias -> {
            // note that there is no physical expression for an alias since the alias
            // only affects the name using in the planning phase and not how the aliased
            // expression is executed
            createPhysicalExpr(expr.expr, input)
        }
        is Column -> {
            val i = input.schema().fields.indexOfFirst { it.name == expr.name }
            if (i == -1) {
                throw SQLException("No column named '${expr.name}'")
            }
            ColumnPExpr(i)
        }
        is CastExpr -> CastPExpr(createPhysicalExpr(expr.expr, input), expr.dataType)

        // comparision
        is Eq -> EqExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //TODO other comparison ops

        // math
//        is Add -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Subtract -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Multiply -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        is Divide -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Modulus -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))

        // boolean
//        is And -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //is Or -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //is Not -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))

        else -> TODO(expr.javaClass.toString())
    }

}