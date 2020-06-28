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
import org.ballistacompute.datasource.InMemoryDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.datatypes.Field
import org.ballistacompute.datatypes.Schema
import org.ballistacompute.fuzzer.Fuzzer
import org.ballistacompute.logical.*
import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AggregateTest {

  @Test
  fun `min accumulator`() {
    val a = MinExpression(ColumnExpression(0)).createAccumulator()
    val values = listOf(10, 14, 4)
    values.forEach { a.accumulate(it) }
    assertEquals(4, a.finalValue())
  }

  @Test
  fun `max accumulator`() {
    val a = MaxExpression(ColumnExpression(0)).createAccumulator()
    val values = listOf(10, 14, 4)
    values.forEach { a.accumulate(it) }
    assertEquals(14, a.finalValue())
  }

  @Test
  fun `sum accumulator`() {
    val a = SumExpression(ColumnExpression(0)).createAccumulator()
    val values = listOf(10, 14, 4)
    values.forEach { a.accumulate(it) }
    assertEquals(28, a.finalValue())
  }
}
