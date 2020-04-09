package org.ballistacompute.physical

import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AggregateTest {

    @Test
    fun `min accumulator`() {
        val a = MinExpression(ColumnExpression(0)).createAccumulator()
        val values = listOf(10, 14, 4)
        values.forEach { a.accumulate(it) }
        assertEquals(4, a.finalValue())
    }

    @Test
    fun `max accumulator`() {
        val a = MaxExpression(ColumnExpression(0)).createAccumulator()
        val values = listOf(10, 14, 4)
        values.forEach { a.accumulate(it) }
        assertEquals(14, a.finalValue())
    }

}