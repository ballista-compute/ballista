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

import java.io.File
import kotlin.test.assertEquals
import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

  val dir = "../testdata"

  val employeeCsv = File(dir, "employee.csv").absolutePath

  @Test
  fun `build logicalPlan manually`() {
    // create a plan to represent the data source
    val csv = CsvDataSource(employeeCsv, null, true, 10)
    // create a plan to represent the scan of the data source (FROM)
    val scan = Scan("employee", csv, listOf())
    // create a plan to represent the selection (WHERE)
    val filterExpr = Eq(col("state"), LiteralString("CO"))
    val selection = Selection(scan, filterExpr)
    // create a plan to represent the projection (SELECT)
    val plan = Projection(selection, listOf(col("id"), col("first_name"), col("last_name")))

    assertEquals(
        "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tScan: employee; projection=None\n",
        format(plan))
  }

  @Test
  fun `build logicalPlan nested`() {
    val plan =
        Projection(
            Selection(
                Scan("employee", CsvDataSource(employeeCsv, null, true, 10), listOf()),
                Eq(col("state"), LiteralString("CO"))),
            listOf(col("id"), col("first_name"), col("last_name")))

    assertEquals(
        "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tScan: employee; projection=None\n",
        format(plan))
  }

  @Test
  fun `build aggregate plan`() {
    // create a plan to represent the data source
    val csv = CsvDataSource(employeeCsv, null, true, 10)

    // create a plan to represent the scan of the data source (FROM)
    val scan = Scan("employee", csv, listOf())

    val groupExpr = listOf(col("state"))
    val aggregateExpr = listOf(Max(cast(col("salary"), ArrowTypes.Int32Type)))
    val plan = Aggregate(scan, groupExpr, aggregateExpr)

    assertEquals(
        "Aggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS Int(32, true)))]\n" +
            "\tScan: employee; projection=None\n",
        format(plan))
  }
}
