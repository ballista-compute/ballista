package org.ballistacompute.physical

import java.lang.UnsupportedOperationException

class MaxPExpr(private val expr: PhysicalExpr) : AggregatePExpr {

    override fun inputExpression(): PhysicalExpr {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MaxAccumulator()
    }

    override fun toString(): String {
        return "MAX($expr)"
    }
}

private class MaxAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMax = when (value) {
                    is Int -> value > this.value as Int
                    is Double -> value > this.value as Double
                    is ByteArray -> throw UnsupportedOperationException("MAX is not implemented for String yet: ${String(value)}")
                    else -> throw UnsupportedOperationException(value.javaClass.name)
                }
                if (isMax) {
                    this.value = value
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }
}