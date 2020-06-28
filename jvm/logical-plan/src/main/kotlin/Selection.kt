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

/** Logical plan representing a selection (a.k.a. filter) against an input */
class Selection(val input: LogicalPlan, val expr: LogicalExpr) : LogicalPlan {
  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<LogicalPlan> {
    // selection does not change the schema of the input
    return listOf(input)
  }

  override fun toString(): String {
    return "Selection: $expr"
  }
}
