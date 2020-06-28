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

import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetDataSourceTest {

  val dir = "../testdata"

  @Test
  fun `read parquet schema`() {
    val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
    assertEquals(
        "Schema(fields=[Field(name=id, dataType=Int(32, true)), Field(name=bool_col, dataType=Bool), Field(name=tinyint_col, dataType=Int(32, true)), Field(name=smallint_col, dataType=Int(32, true)), Field(name=int_col, dataType=Int(32, true)), Field(name=bigint_col, dataType=Int(64, true)), Field(name=float_col, dataType=FloatingPoint(SINGLE)), Field(name=double_col, dataType=FloatingPoint(DOUBLE)), Field(name=date_string_col, dataType=Binary), Field(name=string_col, dataType=Binary), Field(name=timestamp_col, dataType=Binary)])",
        parquet.schema().toString())
  }

  @Test
  @Ignore
  fun `read parquet file`() {
    val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
    val it = parquet.scan(listOf("id")).iterator()
    assertTrue(it.hasNext())

    val batch = it.next()
    assertEquals(1, batch.schema.fields.size)
    assertEquals(8, batch.field(0).size())

    val id = batch.field(0)
    val values = (0..id.size()).map { id.getValue(it) ?: "null" }
    assertEquals("4,5,6,7,2,3,0,1,null", values.joinToString(","))

    assertFalse(it.hasNext())
  }
}
