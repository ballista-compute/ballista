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

package org.ballistacompute.logical

import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datatypes.Schema

/** Represents a scan of a data source */
class Scan(val path: String, val dataSource: DataSource, val projection: List<String>) :
    LogicalPlan {

  val schema = deriveSchema()

  override fun schema(): Schema {
    return schema
  }

  private fun deriveSchema(): Schema {
    val schema = dataSource.schema()
    if (projection.isEmpty()) {
      return schema
    } else {
      return schema.select(projection)
    }
  }

  override fun children(): List<LogicalPlan> {
    return listOf()
  }

  override fun toString(): String {
    return if (projection.isEmpty()) {
      "Scan: $path; projection=None"
    } else {
      "Scan: $path; projection=$projection"
    }
  }
}
