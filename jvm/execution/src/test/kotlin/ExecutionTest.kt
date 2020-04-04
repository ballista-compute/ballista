package org.ballistacompute.execution

import org.ballistacompute.logical.*
import org.apache.arrow.vector.types.pojo.ArrowType
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
        val ctx = ExecutionContext()

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
        val ctx = ExecutionContext()

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
        val ctx = ExecutionContext()

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
        val ctx = ExecutionContext()

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

}