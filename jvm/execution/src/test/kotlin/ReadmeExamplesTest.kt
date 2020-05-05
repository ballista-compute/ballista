package org.ballistacompute.execution

import org.ballistacompute.logical.*

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

/**
 * Example source code for README in this repo.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadmeExamplesTest {

    val employeeCsv = "../testdata/employee.csv"

    @Test
    fun `SQL example`() {

        // Create a context
        val ctx = ExecutionContext(mapOf())

        // Register a CSV data source
        val csv = ctx.csv(employeeCsv)
        ctx.register("employee", csv)

        // Execute a SQL query
        val df = ctx.sql("SELECT id, first_name, last_name FROM employee WHERE state = 'CO'")

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tProjection: #id, #first_name, #last_name, #state\n" +
            "\t\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `DataFrame example`() {

        // Create a context
        val ctx = ExecutionContext(mapOf())

        // Construct a query using the DataFrame API
        val df: DataFrame = ctx.csv(employeeCsv)
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

}