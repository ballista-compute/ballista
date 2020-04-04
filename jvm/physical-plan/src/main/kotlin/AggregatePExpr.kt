package org.ballistacompute.physical

interface AggregatePExpr {
    fun inputExpression(): PhysicalExpr
    fun createAccumulator(): Accumulator
}