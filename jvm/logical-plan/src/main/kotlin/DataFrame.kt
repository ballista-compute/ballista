package org.ballistacompute.logical

import org.ballistacompute.datatypes.Schema

interface DataFrame {

    /** Apply a projection */
    fun project(expr: List<LogicalExpr>): DataFrame

    /** Apply a filter */
    fun filter(expr: LogicalExpr): DataFrame

    /** Aggregate */
    fun aggregate(groupBy: List<LogicalExpr>, aggregateExpr: List<AggregateExpr>): DataFrame

    /** Returns the schema of the data that will be produced by this DataFrame. */
    fun schema(): Schema

    /** Get the logical plan */
    fun logicalPlan() : LogicalPlan

}

class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {

    override fun project(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

    override fun aggregate(groupBy: List<LogicalExpr>,
                           aggregateExpr: List<AggregateExpr>): DataFrame {
        return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
    }

    override fun schema(): Schema {
        return plan.schema()
    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }

}

