package org.ballistacompute.planner

import org.ballistacompute.datatypes.Schema
import org.ballistacompute.datatypes.Field
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.logical.DataFrameImpl
import org.ballistacompute.logical.Scan
import org.ballistacompute.logical.*
import org.ballistacompute.optimizer.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryPlannerTest {

    @Test
    fun `plan aggregate query`() {
        val schema = Schema(listOf(
            Field("passenger_count", ArrowTypes.UInt32Type),
            Field("max_fare", ArrowTypes.DoubleType)
        ))

        val dataSource = InMemoryDataSource(schema, listOf())

        val df = DataFrameImpl(Scan("", dataSource, listOf()))

        val plan = df.aggregate(listOf(col("passenger_count")), listOf(max(col("max_fare")))).logicalPlan()
        assertEquals("Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
                "\tScan: ; projection=None\n", plan.pretty())

        val optimizedPlan = Optimizer().optimize(plan)
        assertEquals("Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
                "\tScan: ; projection=[max_fare, passenger_count]\n", optimizedPlan.pretty())

        val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
        assertEquals("HashAggregateExec: groupExpr=[#1], aggrExpr=[MAX(#0)]\n" +
                "\tScanExec: schema=Schema(fields=[Field(name=max_fare, dataType=FloatingPoint(DOUBLE)), Field(name=passenger_count, dataType=Int(32, false))]), projection=[max_fare, passenger_count]\n", physicalPlan.pretty())

    }


}