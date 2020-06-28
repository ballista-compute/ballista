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

package org.ballistacompute.physical

import kotlin.test.assertEquals
import org.ballistacompute.datatypes.*
import org.ballistacompute.fuzzer.Fuzzer
import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CastExpressionTest {

  @Test
  fun `cast byte to string`() {

    val schema = Schema(listOf(Field("a", ArrowTypes.Int8Type)))

    val a: List<Byte> = listOf(10, 20, 30, Byte.MIN_VALUE, Byte.MAX_VALUE)

    val batch = Fuzzer().createRecordBatch(schema, listOf(a))

    val expr = CastExpression(ColumnExpression(0), ArrowTypes.StringType)
    val result = expr.evaluate(batch)

    assertEquals(a.size, result.size())
    (0 until result.size()).forEach { assertEquals(a[it].toString(), result.getValue(it)) }
  }

  @Test
  fun `cast string to float`() {

    val schema = Schema(listOf(Field("a", ArrowTypes.StringType)))

    val a: List<String> = listOf(Float.MIN_VALUE.toString(), Float.MAX_VALUE.toString())

    val batch = Fuzzer().createRecordBatch(schema, listOf(a))

    val expr = CastExpression(ColumnExpression(0), ArrowTypes.FloatType)
    val result = expr.evaluate(batch)

    assertEquals(a.size, result.size())
    (0 until result.size()).forEach { assertEquals(a[it].toFloat(), result.getValue(it)) }
  }
}
