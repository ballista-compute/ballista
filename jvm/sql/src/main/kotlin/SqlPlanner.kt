package org.ballistacompute.sql

import org.ballistacompute.logical.*

import java.sql.SQLException
import java.util.logging.Logger

/**
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 */
class SqlPlanner {

    private val logger = Logger.getLogger(SqlPlanner::class.simpleName)

    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

        // get a reference to the data source
        val table = tables[select.tableName] ?: throw SQLException("No table named '${select.tableName}'")

        // translate projection sql expressions into logical expressions
        val projectionExpr = select.projection.map { createLogicalExpr(it, table) }

        // build a list of columns referenced in the projection
        val columnNamesInProjection = mutableSetOf<String>()
        projectionExpr.forEach { visit(it, columnNamesInProjection) }
        println("Projection references columns: $columnNamesInProjection")

        // does the filter expression reference anything not in the final projection?
        val columnNamesInSelection = mutableSetOf<String>()
        if (select.selection != null) {
            var filterExpr = createLogicalExpr(select.selection, table)
            visit(filterExpr, columnNamesInSelection)
            val validColumnNames = table.schema().fields.map { it.name }
            columnNamesInSelection.removeIf { name -> !validColumnNames.contains(name) }
            println("Selection references additional columns: $columnNamesInProjection")
        }

        val missing = (columnNamesInSelection - columnNamesInProjection)
        logger.info("** missing: $missing")

        var plan = table

        val aggregateExpr = projectionExpr.filter { it is AggregateExpr }.map { it as AggregateExpr }

        if (select.groupBy.isEmpty() && aggregateExpr.isEmpty()) {

            if (select.selection == null) {
                return plan.project(projectionExpr)
            }

            // if the selection only references outputs from the projection we can simply apply the filter expression
            // to the DataFrame representing the projection
            if (missing.isEmpty()) {
                plan = plan.project(projectionExpr)
                plan = plan.filter(createLogicalExpr(select.selection, plan))
                return plan
            }

            // because the selection references some columns that are not in the projection output we need to create an
            // interim projection that has the additional columns and then we need to remove them after the selection
            // has been applied
            plan = plan.project(projectionExpr + missing.map { Column(it) })
            plan = plan.filter(createLogicalExpr(select.selection, plan))
            return plan.project(projectionExpr.map { Column(it.toField(plan.logicalPlan()).name) })

        } else {

            val projectionWithoutAggregates = projectionExpr.filterNot { it is AggregateExpr }

            if (select.selection != null) {

                val columnNamesInProjectionWithoutAggregates = mutableSetOf<String>()
                projectionWithoutAggregates.forEach { visit(it, columnNamesInProjectionWithoutAggregates) }
                println("Projection without aggregate references columns: $columnNamesInProjectionWithoutAggregates")
                val missing = (columnNamesInSelection - columnNamesInProjectionWithoutAggregates)
                logger.info("** missing: $missing")

                // if the selection only references outputs from the projection we can simply apply the filter expression
                // to the DataFrame representing the projection
                if (missing.isEmpty()) {
                    plan = plan.project(projectionWithoutAggregates)
                    plan = plan.filter(createLogicalExpr(select.selection, plan))
                } else {
                    // because the selection references some columns that are not in the projection output we need to create an
                    // interim projection that has the additional columns and then we need to remove them after the selection
                    // has been applied
                    plan = plan.project(projectionWithoutAggregates + missing.map { Column(it) })
                    plan = plan.filter(createLogicalExpr(select.selection, plan))

                    //plan = plan.project(projectionWithoutAggregates.map { Column(it.toField(plan.logicalPlan()).name) })
                }
            }

            val groupByExpr = select.groupBy.map { createLogicalExpr(it, plan) }
            return plan.aggregate(groupByExpr, aggregateExpr)
        }
    }

    private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
        logger.info("BEFORE visit() $expr, accumulator=$accumulator")
        when (expr) {
            is Column -> accumulator.add(expr.name)
            is Alias -> visit(expr.expr, accumulator)
            is BinaryExpr -> {
                visit(expr.l, accumulator)
                visit(expr.r, accumulator)
            }
            is AggregateExpr -> visit(expr.expr, accumulator)
        }
        logger.info("AFTER visit() $expr, accumulator=$accumulator")
    }

    private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
        return when (expr) {
            is SqlIdentifier -> Column(expr.id)
            is SqlString -> LiteralString(expr.value)
            is SqlLong -> LiteralLong(expr.value)
            is SqlDouble -> LiteralDouble(expr.value)
            is SqlBinaryExpr -> {
                val l = createLogicalExpr(expr.l, input)
                val r = createLogicalExpr(expr.r, input)
                when(expr.op) {
                    // comparison operators
                    "=" -> Eq(l, r)
                    "!=" -> Neq(l, r)
                    ">" -> Gt(l, r)
                    ">=" -> GtEq(l, r)
                    "<" -> Lt(l, r)
                    "<=" -> LtEq(l, r)
                    // boolean operators
                    "AND" -> And(l, r)
                    "OR" -> Or(l, r)
                    // math operators
                    "+" -> Add(l, r)
                    "-" -> Subtract(l, r)
                    "*" -> Multiply(l, r)
                    "/" -> Divide(l, r)
                    "%" -> Modulus(l, r)
                    else -> throw SQLException("Invalid operator ${expr.op}")
                }
            }
            //is SqlUnaryExpr -> when (expr.op) {
            //"NOT" -> Not(createLogicalExpr(expr.l, input))
            //}
            is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
            is SqlFunction -> when(expr.id) {
                "MAX" -> Max(createLogicalExpr(expr.args.first(), input))
                else -> TODO()
            }
            else -> TODO(expr.javaClass.toString())
        }
    }

}