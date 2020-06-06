// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
                "\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `select with filter`() {
        val plan = plan("SELECT state FROM employee WHERE state = 'CA'")
        assertEquals("Selection: #state = 'CA'\n" +
                "\tProjection: #state\n" +
                "\t\tScan: ; projection=None\n", format(plan))
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
    fun `select filter on projection`() {
        val plan = plan("SELECT last_name AS foo FROM employee WHERE foo = 'Einstein'")
        assertEquals("Selection: #foo = 'Einstein'\n" +
                "\tProjection: #last_name as foo\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `select filter on projection and not`() {
        val plan = plan("SELECT last_name AS foo " +
                "FROM employee " +
                "WHERE foo = 'Einstein' AND state = 'CA'")
        assertEquals("Projection: #foo\n" +
                "\tSelection: #foo = 'Einstein' AND #state = 'CA'\n" +
                "\t\tProjection: #last_name as foo, #state\n" +
                "\t\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `plan aggregate query`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals("Projection: #0, #1\n" +
                "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `plan aggregate query aggr first`() {
        val plan = plan("SELECT MAX(salary), state FROM employee GROUP BY state")
        assertEquals("Projection: #1, #0\n" +
                "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `plan aggregate query with filter`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee WHERE salary > 50000 GROUP BY state")
        assertEquals("Projection: #0, #1\n" +
                "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
                "\t\tSelection: #salary > 50000\n" +
                "\t\t\tProjection: #state, #salary\n" +
                "\t\t\t\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `plan aggregate query with cast`() {
        val plan = plan("SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state")
        assertEquals("Projection: #0, #1\n" +
                "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS FloatingPoint(DOUBLE)))]\n" +
                "\t\tScan: ; projection=None\n", format(plan))
    }

    private fun plan(sql: String) : LogicalPlan {
        println("parse() $sql")

        val tokens = SqlTokenizer(sql).tokenize()
        println(tokens)

        val parsedQuery = SqlParser(tokens).parse()
        println(parsedQuery)

       val tables = mapOf("employee" to DataFrameImpl(Scan("", CsvDataSource(employeeCsv, null, 1024), listOf())))

        val df = SqlPlanner().createDataFrame(parsedQuery as SqlSelect, tables)

        val plan = df.logicalPlan()
        println(format(plan))

        return plan
    }
}

