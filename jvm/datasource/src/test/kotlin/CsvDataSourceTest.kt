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

package org.ballistacompute.datasource

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvDataSourceTest {

    val dir = "../testdata"

    @Test
    fun `read csv`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true,1024)
        val result = csv.scan(listOf())
        result.asSequence().forEach {
            val field = it.field(0)
            println(field.size())
            assert(field.size() == 3)
        }
    }

}

