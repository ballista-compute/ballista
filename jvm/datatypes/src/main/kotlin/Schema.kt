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

import java.lang.IllegalArgumentException
import org.apache.arrow.vector.types.pojo.ArrowType

object SchemaConverter {
  fun fromArrow(arrowSchema: org.apache.arrow.vector.types.pojo.Schema): Schema {
    val fields = arrowSchema.fields.map { Field(it.name, it.fieldType.type) }
    return Schema(fields)
  }
}

data class Schema(val fields: List<Field>) {

  fun toArrow(): org.apache.arrow.vector.types.pojo.Schema {
    return org.apache.arrow.vector.types.pojo.Schema(fields.map { it.toArrow() })
  }

  fun project(indices: List<Int>): Schema {
    return Schema(indices.map { fields[it] })
  }

  fun select(names: List<String>): Schema {
    val f = mutableListOf<Field>()
    names.forEach { name ->
      val m = fields.filter { it.name == name }
      if (m.size == 1) {
        f.add(m[0])
      } else {
        throw IllegalArgumentException()
      }
    }
    return Schema(f)
  }
}

data class Field(val name: String, val dataType: ArrowType) {
  fun toArrow(): org.apache.arrow.vector.types.pojo.Field {
    val fieldType = org.apache.arrow.vector.types.pojo.FieldType(true, dataType, null)
    return org.apache.arrow.vector.types.pojo.Field(name, fieldType, listOf())
  }
}
