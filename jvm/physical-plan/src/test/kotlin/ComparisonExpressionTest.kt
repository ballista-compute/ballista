package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.ballistacompute.datatypes.*
import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ComparisonExpressionTest {

    @Test
    fun `gteq bytes`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.Int8Type),
                Field("b", ArrowTypes.Int8Type)
        ))

        val a: List<Byte> = listOf(10, 20, 30, Byte.MIN_VALUE, Byte.MAX_VALUE)
        val b: List<Byte> = listOf(10, 30, 20, Byte.MAX_VALUE, Byte.MIN_VALUE)

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }

    @Test
    fun `gteq shorts`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.Int16Type),
                Field("b", ArrowTypes.Int16Type)
        ))

        val a: List<Short> = listOf(111, 222, 333, Short.MIN_VALUE, Short.MAX_VALUE)
        val b: List<Short> = listOf(111, 333, 222, Short.MAX_VALUE, Short.MIN_VALUE)

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }

    @Test
    fun `gteq ints`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.Int32Type),
                Field("b", ArrowTypes.Int32Type)
        ))

        val a: List<Int> = listOf(111, 222, 333, Int.MIN_VALUE, Int.MAX_VALUE)
        val b: List<Int> = listOf(111, 333, 222, Int.MAX_VALUE, Int.MIN_VALUE)

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }

    @Test
    fun `gteq longs`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.Int64Type),
                Field("b", ArrowTypes.Int64Type)
        ))

        val a: List<Long> = listOf(111, 222, 333, Long.MIN_VALUE, Long.MAX_VALUE)
        val b: List<Long> = listOf(111, 333, 222, Long.MAX_VALUE, Long.MIN_VALUE)

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }

    @Test
    fun `gteq doubles`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.DoubleType),
                Field("b", ArrowTypes.DoubleType)
        ))

        val a: List<Double> = listOf(0.0, 1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN)
        val b = a.reversed()

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }

    @Test
    fun `gteq strings`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.StringType),
                Field("b", ArrowTypes.StringType)
        ))

        val a: List<String> = listOf("aaa", "bbb", "ccc")
        val b: List<String> = listOf("aaa", "ccc", "bbb")

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }


    private fun createRecordBatch(schema: Schema, columns: List<List<Any?>>): RecordBatch {

        val rowCount = columns[0].size

        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()

        (0 until rowCount).forEach { row ->
            (0 until columns.size).forEach { col ->
                val v = root.getVector(col)
                val value = columns[col][row]
                when (v) {
                    is VarCharVector -> v.set(row, (value as String).toByteArray())
                    is TinyIntVector -> v.set(row, value as Byte)
                    is SmallIntVector -> v.set(row, value as Short)
                    is IntVector -> v.set(row, value as Int)
                    is BigIntVector -> v.set(row, value as Long)
                    is Float4Vector -> v.set(row, value as Float)
                    is Float8Vector -> v.set(row, value as Double)
                    else -> TODO()
                }
            }
        }
        root.rowCount = rowCount

        return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
    }

}