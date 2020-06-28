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

package org.ballistacompute.execution

import kotlin.test.assertEquals
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

/** Example source code for README in this repo. */
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
    val df: DataFrame =
        ctx.csv(employeeCsv)
            .filter(col("state") eq lit("CO"))
            .project(listOf(col("id"), col("first_name"), col("last_name")))

    val expected =
        "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tScan: ../testdata/employee.csv; projection=None\n"

    assertEquals(expected, format(df.logicalPlan()))
  }
}
