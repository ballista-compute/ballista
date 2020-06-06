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

package org.ballistacompute.optimizer

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OptimizerTest {

    @Test
    fun `projection push down`() {

        val df = csv()
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tScan: employee; projection=[first_name, id, last_name]\n"

        assertEquals(expected, optimizedPlan.pretty())
    }

    @Test
    fun `projection push down with selection`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        println(df.logicalPlan().pretty());

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())
        println(optimizedPlan.pretty());

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: employee; projection=[first_name, id, last_name, state]\n"

        assertEquals(expected, optimizedPlan.pretty())
    }

    @Test
    fun `projection push down with  aggregate query`() {

        val df = csv()
                .aggregate(listOf(col("state")), listOf(Min(col("salary")), Max(col("salary")), Count(col("salary"))))

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        assertEquals(
                "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n" +
                        "\tScan: employee; projection=[salary, state]\n", format(optimizedPlan))
    }

    private fun csv() : DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, 1024), listOf()))
    }
}