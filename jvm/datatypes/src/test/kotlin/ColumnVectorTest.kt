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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColumnVectorTest {

    @Test
    fun `build int vector`() {
        val size = 10
        val fieldVector = IntVector("foo", RootAllocator(Long.MAX_VALUE))
        fieldVector.allocateNew(size)
        fieldVector.valueCount = size
        val b = ArrowVectorBuilder(fieldVector)
        (0 until size).forEach {
            b.set(it, it)
        }
        val v = b.build()

        assertEquals(10, v.size())
        (0 until v.size()).forEach {
            assertEquals(it, v.getValue(it))
        }
    }

}