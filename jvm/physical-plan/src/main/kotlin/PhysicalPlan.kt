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

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema


/**
 * A physical plan represents an executable piece of code that will produce data.
 */
interface PhysicalPlan {

    fun schema(): Schema

    /**
     * Execute a physical plan and produce a series of record batches.
     */
    fun execute(): Sequence<RecordBatch>

    /**
     * Returns the children (inputs) of this physical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    fun children(): List<PhysicalPlan>

    fun pretty(): String {
        return format(this)
    }

}

/** Format a logical plan in human-readable form */
private fun format(plan: PhysicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent+1)) }
    return b.toString()
}




