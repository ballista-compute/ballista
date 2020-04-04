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

        // build a list of columns referenced in the projection
        val projectionExpr = select.projection.map { createLogicalExpr(it, table) }

        val columnNames = mutableSetOf<String>()
        projectionExpr.forEach { visit(it, columnNames) }
        if (select.selection != null) {
            //TODO this is very hacky and incorrect
            var filterExpr = createLogicalExpr(select.selection, table)
            visit(filterExpr, columnNames)
        }
        println("Projection and selection references columns: $columnNames")

        // apply projection to table
        var plan = table.project( columnNames.map { Column(it) })

        // apply filter
        if (select.selection != null) {
            plan = plan.filter(createLogicalExpr(select.selection, plan))
        }

        val aggregateExpr = projectionExpr.filter { it is AggregateExpr }.map { it as AggregateExpr }

        return if (select.groupBy.isEmpty() && aggregateExpr.isEmpty()) {
            // apply original projection
            plan.project(projectionExpr)
        } else {
            val groupByExpr = select.groupBy.map { createLogicalExpr(it, plan) }
            plan.aggregate(groupByExpr, aggregateExpr)
        }
    }

//    /**
//     * Apply project and selection only, preserving all fields that are needed for later operations such
//     * as aggregate or join.
//     */
//    fun planProjectionAndSelection(select: SqlSelect, df: DataFrame) : DataFrame {
//
//        // we need to build a list of all columns referenced in the
//        val columnNames = mutableSetOf<String>()
//        visit(filterExpr, columnNames)
//        logger.info("selection references columns: $columnNames")
//
//
//        // create the logical expressions for the projection
//        val projectionExpr = select.projection.map { createLogicalExpr(it, df) }
//
//        // get a list of columns references in the projection expression
//        val columnsInProjection = projectionExpr
//                .map { it.toField(df.logicalPlan()).name}
//                .toSet()
//        logger.info("projection references columns $columnsInProjection")
//
//        val projectionWithoutAggregates = projectionExpr.filterNot { it is AggregateExpr }
//
//        if (select.selection == null) {
//            // if there is no selection then we can just return the projection
//            return df.project(projectionWithoutAggregates)
//        }
//
//        // create the logical expression to represent the filter
//        val filterExpr = createLogicalExpr(select.selection, df)
//
//        // get a list of columns referenced in the filter expression
//        val columnNames = mutableSetOf<String>()
//        visit(filterExpr, columnNames)
//        logger.info("selection references columns: $columnNames")
//
//        // determine if the filter references any columns not in the projection
//        val missing = (columnNames - columnsInProjection)
//        logger.info("** missing: $missing")
//
//        // if the selection only references outputs from the projection we can simply apply the filter expression
//        // to the DataFrame representing the projection
//        if (missing.size == 0) {
//            return df.project(projectionWithoutAggregates)
//                .filter(filterExpr)
//        }
//
//        // because the selection references some columns that are not in the projection output we need to create an
//        // interim projection that has the additional columns and then we need to remove them after the selection
//        // has been applied
//        return df.project(projectionWithoutAggregates + missing.map { Column(it) })
//            .filter(filterExpr)
//            .project(projectionWithoutAggregates.map { Column(it.toField(df.logicalPlan()).name) })
//    }

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