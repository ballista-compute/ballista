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