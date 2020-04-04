package org.ballistacompute.planner

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.ballistacompute.datasource.InMemoryDataSource
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
            Field.nullable("passenger_count", ArrowType.Utf8()),
            Field.nullable("max_fare", ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ))

        val dataSource = InMemoryDataSource(schema, listOf())

        val df = DataFrameImpl(Scan("", dataSource, listOf()))

        val plan = df.aggregate(listOf(col("passenger_count")), listOf(max(col("max_fare")))).logicalPlan()
        assertEquals(plan.pretty(), "Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
                "\tScan: ; projection=None\n")

        val optimizedPlan = Optimizer().optimize(plan)
        assertEquals(optimizedPlan.pretty(), "Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
                "\tScan: ; projection=[max_fare, passenger_count]\n")

        val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
        assertEquals(physicalPlan.pretty(), "HashAggregateExec: groupExpr=[#0], aggrExpr=[MAX(#1)]\n" +
                "\tScanExec: schema=Schema<max_fare: FloatingPoint(DOUBLE), passenger_count: Utf8>, projection=[max_fare, passenger_count]\n")

    }


}