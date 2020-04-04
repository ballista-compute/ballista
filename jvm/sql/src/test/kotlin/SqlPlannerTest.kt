package org.ballistacompute.sql

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.logical.format
import org.ballistacompute.logical.DataFrameImpl
import org.ballistacompute.logical.Scan
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlPlannerTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `simple select`() {
        val plan = plan("SELECT state FROM employee")
        assertEquals("Projection: #state\n" +
                "\tProjection: #state\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `select with filter`() {
        val plan = plan("SELECT state FROM employee WHERE state = 'CA'")
        assertEquals("Projection: #state\n" +
                "\tSelection: #state = 'CA'\n" +
                "\t\tProjection: #state\n" +
                "\t\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `select with filter not in projection`() {
        val plan = plan("SELECT last_name FROM employee WHERE state = 'CA'")
        assertEquals("Projection: #last_name\n" +
                "\tSelection: #state = 'CA'\n" +
                "\t\tProjection: #last_name, #state\n" +
                "\t\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    @Ignore
    fun `select filter on projection`() {
        val plan = plan("SELECT last_name AS foo FROM employee WHERE foo = 'Einstein'")
        assertEquals("Selection: #foo = 'Einstein'\n" +
                "\t\tProjection: #last_name as foo\n" +
                "\t\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    @Ignore
    fun `select filter on projection and not`() {
        val plan = plan("SELECT last_name AS foo " +
                "FROM employee " +
                "WHERE foo = 'Einstein' AND state = 'CA'")
        assertEquals("TBD\n", format(plan))
    }

    @Test
    fun `plan aggregate query`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals("Aggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
                "\tProjection: #state, #salary\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `plan aggregate query with filter`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee WHERE salary > 50000 GROUP BY state")
        assertEquals("Aggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
                "\tSelection: #salary > 50000\n" +
                "\t\tProjection: #state, #salary\n" +
                "\t\t\tScan: ; projection=None\n", format(plan))
    }

    private fun plan(sql: String) : LogicalPlan {
        println("parse() $sql")

        val tokens = SqlTokenizer(sql).tokenize()
        println(tokens)

        val parsedQuery = SqlParser(tokens).parse()
        println(parsedQuery)

       val tables = mapOf("employee" to DataFrameImpl(Scan("", CsvDataSource(employeeCsv, 1024), listOf())))

        val df = SqlPlanner().createDataFrame(parsedQuery as SqlSelect, tables)

        val plan = df.logicalPlan()
        println(format(plan))

        return plan
    }
}

