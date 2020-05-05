package org.ballistacompute.execution

import org.ballistacompute.logical.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.datatypes.Field
import org.ballistacompute.datatypes.Schema
import org.ballistacompute.fuzzer.Fuzzer
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExecutionTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `employees in CO using DataFrame`() {
        // Create a context
        val ctx = ExecutionContext(mapOf())

        // Construct a query using the DataFrame API
        val df = ctx.csv(employeeCsv)
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val batches = ctx.execute(df).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals(
                "2,Gregg,Langford\n" +
                "3,John,Travis\n", batch.toCSV())
    }

    @Test
    fun `employees in CA using SQL`() {
        // Create a context
        val ctx = ExecutionContext(mapOf())

        val employee = ctx.csv(employeeCsv)
        ctx.register("employee", employee)

        // Construct a query using the DataFrame API
        val df = ctx.sql("SELECT id, first_name, last_name FROM employee WHERE state = 'CA'")

        val batches = ctx.execute(df).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("1,Bill,Hopkins\n"
                , batch.toCSV())
    }

    @Test
    fun `aggregate query`() {
        // Create a context
        val ctx = ExecutionContext(mapOf())

        // construct a query using the DataFrame API
        val df = ctx.csv(employeeCsv)
            .aggregate(listOf(col("state")), listOf(Max(cast(col("salary"), ArrowType.Int(32, true)))))

        val batches = ctx.execute(df).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        val expected =
                "CO,11500\n" +
                "CA,12000\n"
        assertEquals(expected
                , batch.toCSV())
    }

    @Test
    fun `bonuses in CA using SQL and DataFrame`() {
        // Create a context
        val ctx = ExecutionContext(mapOf())

        // construct a query using the DataFrame API
        val caEmployees = ctx.csv(employeeCsv)
                .filter(col("state") eq lit("CA"))
                .project(listOf(col("id"), col("first_name"), col("last_name"), col("salary")))

        // register the DataFrame as a table
        ctx.register("ca_employees", caEmployees)

        // Construct a query using the DataFrame API
        val df = ctx.sql("SELECT id, first_name, last_name, salary FROM ca_employees")
        //val df = ctx.sql("SELECT id, first_name, last_name, salary * 0.1 AS bonus FROM ca_employees")

        val batches = ctx.execute(df).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("1,Bill,Hopkins,12000\n"
                , batch.toCSV())
    }

    @Test
    fun `min max sum float`() {
        val schema = Schema(listOf(
                Field("a", ArrowTypes.StringType),
                Field("b", ArrowTypes.FloatType)
        ))

        //val batch = Fuzzer().createRecordBatch(schema, 1024)
        val input = Fuzzer().createRecordBatch(schema, listOf(
                listOf("a", "a", "b", "b"),
                listOf(1.0f, 2.0f, 4.0f, 3.0f)
        ))

        val dataSource = InMemoryDataSource(schema, listOf(input))

        val ctx = ExecutionContext(mapOf())
        val logicalPlan = DataFrameImpl(Scan("", dataSource, listOf()))
                .aggregate(listOf(col("a")),
                        listOf(
                                Min(col("b")),
                                Max(col("b")),
                                Sum(col("b"))
                        ))
                .logicalPlan()

        val batches = ctx.execute(logicalPlan).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("a,1.0,2.0,3.0\n" +
                "b,3.0,4.0,7.0\n"
                , batch.toCSV())

    }

    @Test
    fun `float math`() {
        val schema = Schema(listOf(
                Field("a", ArrowTypes.FloatType),
                Field("b", ArrowTypes.FloatType)
        ))

        //val batch = Fuzzer().createRecordBatch(schema, 1024)
        val input = Fuzzer().createRecordBatch(schema, listOf(
                listOf(1.0f, 2.0f, 4.0f, 3.0f),
                listOf(11.0f, 22.0f, 44.0f, 33.0f)
        ))

        val dataSource = InMemoryDataSource(schema, listOf(input))

        val ctx = ExecutionContext(mapOf())
        val logicalPlan = DataFrameImpl(Scan("", dataSource, listOf()))
                .project(
                        listOf(
                                Add(col("a"), col("b")),
                                Subtract(col("a"), col("b")),
                                Multiply(col("a"), col("b")),
                                Divide(col("a"), col("b"))
                        ))
                .logicalPlan()

        val batches = ctx.execute(logicalPlan).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("12.0,-10.0,11.0,0.09090909\n" +
                "24.0,-20.0,44.0,0.09090909\n" +
                "48.0,-40.0,176.0,0.09090909\n" +
                "36.0,-30.0,99.0,0.09090909\n"
                , batch.toCSV())

    }

    @Test
    fun `boolean expressions`() {
        val schema = Schema(listOf(
                Field("a", ArrowTypes.BooleanType),
                Field("b", ArrowTypes.BooleanType)
        ))

        val input = Fuzzer().createRecordBatch(schema, listOf(
                listOf(false, false, true, true),
                listOf(false, true, false, true)
        ))

        val dataSource = InMemoryDataSource(schema, listOf(input))

        val ctx = ExecutionContext(mapOf())
        val logicalPlan = DataFrameImpl(Scan("", dataSource, listOf()))
                .project(
                        listOf(
                                And(col("a"), col("b")),
                                Or(col("a"), col("b"))
                        ))
                .logicalPlan()

        val batches = ctx.execute(logicalPlan).asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("false,false\n" +
                "false,true\n" +
                "false,true\n" +
                "true,true\n"
                , batch.toCSV())

    }
}