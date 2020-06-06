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

package org.ballistacompute.fuzzer

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FuzzerTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `fuzzer example`() {
        val csv = CsvDataSource(employeeCsv, null, 10)
        val input = DataFrameImpl(Scan("employee.csv", csv, listOf()))
        val fuzzer = Fuzzer()


        (0 until 50).forEach {
            println(fuzzer.createPlan(input, 0, 6, 1).logicalPlan().pretty())

//            val expr = fuzzer.createExpression(input, 0, 5)
//            println(expr)
        }
    }

}