package org.ballistacompute.physical

import org.ballistacompute.datatypes.*
import org.ballistacompute.fuzzer.Fuzzer

import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CastExpressionTest {
    
    @Test
    fun `cast byte to string`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.Int8Type)
        ))

        val a: List<Byte> = listOf(10, 20, 30, Byte.MIN_VALUE, Byte.MAX_VALUE)

        val batch = Fuzzer().createRecordBatch(schema, listOf(a))

        val expr = CastExpression(ColumnExpression(0), ArrowTypes.StringType)
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(a[it].toString(), result.getValue(it))
        }
    }

    @Test
    fun `cast string to float`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.StringType)
        ))

        val a: List<String> = listOf(Float.MIN_VALUE.toString(), Float.MAX_VALUE.toString())

        val batch = Fuzzer().createRecordBatch(schema, listOf(a))

        val expr = CastExpression(ColumnExpression(0), ArrowTypes.FloatType)
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(a[it].toFloat(), result.getValue(it))
        }
    }
}