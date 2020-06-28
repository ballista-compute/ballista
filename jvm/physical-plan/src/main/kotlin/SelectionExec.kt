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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ArrowVectorBuilder
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema
import org.ballistacompute.physical.expressions.Expression

/** Execute a selection. */
class SelectionExec(val input: PhysicalPlan, val expr: Expression) : PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    val input = input.execute()
    return input.map { batch ->
      val result = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector
      val schema = batch.schema
      val columnCount = batch.schema.fields.size
      val filteredFields = (0 until columnCount).map { filter(batch.field(it), result) }
      val fields = filteredFields.map { ArrowFieldVector(it) }
      RecordBatch(schema, fields)
    }
  }

  private fun filter(v: ColumnVector, selection: BitVector): FieldVector {
    val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
    filteredVector.allocateNew()

    val builder = ArrowVectorBuilder(filteredVector)

    var count = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        builder.set(count, v.getValue(it))
        count++
      }
    }
    filteredVector.valueCount = count
    return filteredVector
  }
}
