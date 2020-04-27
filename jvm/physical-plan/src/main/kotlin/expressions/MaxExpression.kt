package org.ballistacompute.physical.expressions

import java.lang.UnsupportedOperationException

class MaxExpression(private val expr: Expression) : AggregateExpression {

    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MaxAccumulator()
    }

    override fun toString(): String {
        return "MAX($expr)"
    }
}

class MaxAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMax = when (value) {
                    is Byte -> value > this.value as Byte
                    is Short -> value > this.value as Short
                    is Int -> value > this.value as Int
                    is Long -> value > this.value as Long
                    is Float -> value > this.value as Float
                    is Double -> value > this.value as Double
                    is String -> value > this.value as String
                    else -> throw UnsupportedOperationException("MAX is not implemented for data type: ${value.javaClass.name}")
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