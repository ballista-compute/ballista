package org.ballistacompute.execution

import org.ballistacompute.logical.format
import org.ballistacompute.physical.*
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExecutionSqlTest {

    val employeeCsv = "../testdata/employee.csv"

    @Test
    fun `simple SELECT`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT id FROM employee")

        val expected =
            "Projection: #id\n" +
            "\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `SELECT with WHERE`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT id FROM employee WHERE state = 'CO'")

        val expected =
            "Projection: #id\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tProjection: #id, #state\n" +
            "\t\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `SELECT with aliased binary expression`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT salary * 0.1 AS bonus FROM employee")

        val expected =
                "Projection: #salary * 0.1 as bonus\n" +
                "\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `Selection referencing aliased expression`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT salary AS annual_salary FROM employee WHERE annual_salary > 1000 AND state = 'CO")

        val expected =
            "Projection: #annual_salary\n" +
            "\tSelection: #annual_salary > 1000 AND #state = 'CO'\n" +
            "\t\tProjection: #salary as annual_salary, #state\n" +
            "\t\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    private fun createContext() : ExecutionContext {
        val ctx = ExecutionContext()
        ctx.register("employee", ctx.csv(employeeCsv))
        return ctx
    }
}