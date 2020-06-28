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

import org.ballistacompute.datatypes.Schema

/** Logical plan representing a projection (evaluating a list of expressions) against an input */
class Projection(val input: LogicalPlan, val expr: List<LogicalExpr>) : LogicalPlan {
  override fun schema(): Schema {
    return Schema(expr.map { it.toField(input) })
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Projection: ${ expr.map { it.toString() }.joinToString(", ") }"
  }
}
