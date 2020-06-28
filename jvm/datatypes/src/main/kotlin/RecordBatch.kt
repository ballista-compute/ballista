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

package org.ballistacompute.datatypes

/** Batch of data organized in columns. */
class RecordBatch(val schema: Schema, val fields: List<ColumnVector>) {

  fun rowCount() = fields.first().size()

  fun columnCount() = fields.size

  /** Access one column by index */
  fun field(i: Int): ColumnVector {
    return fields[i]
  }

  /** Useful for testing */
  fun toCSV(): String {
    val b = StringBuilder()
    val columnCount = schema.fields.size

    (0 until rowCount()).forEach { rowIndex ->
      (0 until columnCount).forEach { columnIndex ->
        if (columnIndex > 0) {
          b.append(",")
        }
        val v = fields[columnIndex]
        val value = v.getValue(rowIndex)
        if (value == null) {
          b.append("null")
        } else if (value is ByteArray) {
          b.append(String(value))
        } else {
          b.append(value)
        }
      }
      b.append("\n")
    }
    return b.toString()
  }

  override fun toString(): String {
    return toCSV()
  }
}
