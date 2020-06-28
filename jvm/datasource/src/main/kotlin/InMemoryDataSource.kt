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

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema

class InMemoryDataSource(val schema: Schema, val data: List<RecordBatch>) : DataSource {

  override fun schema(): Schema {
    return schema
  }

  override fun scan(projection: List<String>): Sequence<RecordBatch> {
    val projectionIndices =
        projection.map { name -> schema.fields.indexOfFirst { it.name == name } }
    return data.asSequence().map { batch ->
      RecordBatch(schema, projectionIndices.map { i -> batch.field(i) })
    }
  }
}
