package org.ballistacompute.planner

import org.ballistacompute.datatypes.Schema
import org.ballistacompute.logical.*
import org.ballistacompute.physical.*
import org.ballistacompute.physical.expressions.*
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
                val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
                ProjectionExec(input, projectionSchema, projectionExpr)
            }
            is Aggregate -> {
                val input = createPhysicalPlan(plan.input)
                val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
                val aggregateExpr = plan.aggregateExpr.map {
                    when (it) {
                        is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
                        is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
                        else -> throw java.lang.IllegalStateException("Unsupported aggregate function: $it")
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
    fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): Expression = when (expr) {
        is LiteralLong -> LiteralLongExpression(expr.n)
        is LiteralDouble -> LiteralDoubleExpression(expr.n)
        is LiteralString -> LiteralStringExpression(expr.str)
        is ColumnIndex -> ColumnExpression(expr.i)
        is Alias -> {
            // note that there is no physical expression for an alias since the alias
            // only affects the name using in the planning phase and not how the aliased
            // expression is executed
            createPhysicalExpr(expr.expr, input)
        }
        is Column -> {
            val i = input.schema().fields.indexOfFirst {it.name == expr.name }
            if (i == -1) {
                throw SQLException("No column named '${expr.name}'")
            }
            ColumnExpression(i)
        }
        is CastExpr -> CastExpression(createPhysicalExpr(expr.expr, input), expr.dataType)

        // comparision
        is Eq -> EqExpression(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //TODO other comparison ops

        // math
//        is Add -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Subtract -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Multiply -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        is Divide -> MultExpression(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
//        is Modulus -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))

        // boolean
//        is And -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //is Or -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //is Not -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))

        else -> TODO(expr.javaClass.toString())
    }

}