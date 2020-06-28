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

package org.ballistacompute.client

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun test() {

        val df = DataFrameImpl(Scan("", CsvDataSource("", null, true,0), listOf()))

        val df2 = df
            .filter(col("a") eq lit(123))
            .project(listOf(col("a"), col("b"), col("c")))

        val client = Client("localhost", 50001)
        client.execute(df2.logicalPlan())
    }
}