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

package org.ballistacompute.logical

import org.ballistacompute.datasource.CsvDataSource

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun `build DataFrame`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                        "\tSelection: #state = 'CO'\n" +
                        "\t\tScan: employee; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `multiplier and alias`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(
                        col("id"),
                        col("first_name"),
                        col("last_name"),
                        col("salary"),
                        (col("salary") mult lit(0.1)) alias "bonus"))
                .filter(col("bonus") gt lit(1000))

        val expected =
                "Selection: #bonus > 1000\n" +
                        "\tProjection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus\n" +
                        "\t\tSelection: #state = 'CO'\n" +
                        "\t\t\tScan: employee; projection=None\n"

        val actual = format(df.logicalPlan())

        assertEquals(expected, actual)
    }

    @Test
    fun `aggregate query`() {

        val df = csv()
                .aggregate(listOf(col("state")), listOf(Min(col("salary")), Max(col("salary")), Count(col("salary"))))

        assertEquals(
                "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n" +
                        "\tScan: employee; projection=None\n", format(df.logicalPlan()))
    }

    private fun csv() : DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, 1024), listOf()))
    }
}