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

package org.ballistacompute.protobuf.test

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.ballistacompute.protobuf.ProtobufDeserializer
import org.ballistacompute.protobuf.ProtobufSerializer
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SerdeTest {

    @Test
    fun `Convert plan to protobuf`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val logicalPlan = roundtrip(df)

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
                    "\tSelection: #state = 'CO'\n" +
                    "\t\tScan: src/test/resources/employee.csv; projection=None\n"

        assertEquals(expected, format(logicalPlan))
    }

    private fun roundtrip(df: DataFrame) : LogicalPlan {
        val protobuf = ProtobufSerializer().toProto(df.logicalPlan())
        return ProtobufDeserializer().fromProto(protobuf)
    }

    private fun csv(): DataFrame {
        val employeeCsv = "src/test/resources/employee.csv"
        val csv = CsvDataSource(employeeCsv, null, 1024)
        println(csv.schema())
        return DataFrameImpl(Scan(employeeCsv, csv, listOf()))
    }
}