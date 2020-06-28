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

package org.ballistacompute.fuzzer

import java.lang.IllegalStateException
import kotlin.random.Random
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema
import org.ballistacompute.logical.*

class Fuzzer {

  private val rand = Random(0)

  private val enhancedRandom = EnhancedRandom(rand)

  /** Create a list of random values based on the provided data type */
  fun createValues(arrowType: ArrowType, n: Int): List<Any?> {
    return when (arrowType) {
      ArrowTypes.Int8Type -> (0 until n).map { enhancedRandom.nextByte() }
      ArrowTypes.Int16Type -> (0 until n).map { enhancedRandom.nextShort() }
      ArrowTypes.Int32Type -> (0 until n).map { enhancedRandom.nextInt() }
      ArrowTypes.Int64Type -> (0 until n).map { enhancedRandom.nextLong() }
      ArrowTypes.FloatType -> (0 until n).map { enhancedRandom.nextFloat() }
      ArrowTypes.DoubleType -> (0 until n).map { enhancedRandom.nextDouble() }
      ArrowTypes.StringType -> (0 until n).map { enhancedRandom.nextString(rand.nextInt(64)) }
      else -> throw IllegalStateException()
    }
  }

  /** Create a RecordBatch containing random data based on the provided schema */
  fun createRecordBatch(schema: Schema, n: Int): RecordBatch {
    val columns = schema.fields.map { it.dataType }.map { createValues(it, n) }
    return createRecordBatch(schema, columns)
  }

  /** Create a RecordBatch containing the specified values. */
  fun createRecordBatch(schema: Schema, columns: List<List<Any?>>): RecordBatch {

    val rowCount = columns[0].size

    val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
    root.allocateNew()

    (0 until rowCount).forEach { row ->
      (0 until columns.size).forEach { col ->
        val v = root.getVector(col)
        val value = columns[col][row]
        when (v) {
          is BitVector -> v.set(row, if (value as Boolean) 1 else 0)
          is TinyIntVector -> v.set(row, value as Byte)
          is SmallIntVector -> v.set(row, value as Short)
          is IntVector -> v.set(row, value as Int)
          is BigIntVector -> v.set(row, value as Long)
          is Float4Vector -> v.set(row, value as Float)
          is Float8Vector -> v.set(row, value as Double)
          is VarCharVector -> v.set(row, (value as String).toByteArray())
          else -> throw IllegalStateException()
        }
      }
    }
    root.rowCount = rowCount

    return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
  }

  fun createPlan(input: DataFrame, depth: Int, maxDepth: Int, maxExprDepth: Int): DataFrame {
    return if (depth == maxDepth) {
      input
    } else {
      val child = createPlan(input, depth + 1, maxDepth, maxExprDepth)
      when (rand.nextInt(2)) {
        0 -> {
          val exprCount = 1.rangeTo(rand.nextInt(1, 5))
          child.project(exprCount.map { createExpression(child, 0, maxExprDepth) })
        }
        1 -> child.filter(createExpression(input, 0, maxExprDepth))
        else -> throw IllegalStateException()
      }
    }
  }

  fun createExpression(input: DataFrame, depth: Int, maxDepth: Int): LogicalExpr {
    return if (depth == maxDepth) {
      // return a leaf node
      when (rand.nextInt(4)) {
        0 -> ColumnIndex(rand.nextInt(input.schema().fields.size))
        1 -> LiteralDouble(enhancedRandom.nextDouble())
        2 -> LiteralLong(enhancedRandom.nextLong())
        3 -> LiteralString(enhancedRandom.nextString(rand.nextInt(64)))
        else -> throw IllegalStateException()
      }
    } else {
      // binary expressions
      val l = createExpression(input, depth + 1, maxDepth)
      val r = createExpression(input, depth + 1, maxDepth)
      return when (rand.nextInt(8)) {
        0 -> Eq(l, r)
        1 -> Neq(l, r)
        2 -> Lt(l, r)
        3 -> LtEq(l, r)
        4 -> Gt(l, r)
        5 -> GtEq(l, r)
        6 -> And(l, r)
        7 -> Or(l, r)
        else -> throw IllegalStateException()
      }
    }
  }
}

class EnhancedRandom(val rand: Random) {

  private val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

  fun nextByte(): Byte {
    return when (rand.nextInt(5)) {
      0 -> Byte.MIN_VALUE
      1 -> Byte.MAX_VALUE
      2 -> -0
      3 -> 0
      4 -> rand.nextInt().toByte()
      else -> throw IllegalStateException()
    }
  }

  fun nextShort(): Short {
    return when (rand.nextInt(5)) {
      0 -> Short.MIN_VALUE
      1 -> Short.MAX_VALUE
      2 -> -0
      3 -> 0
      4 -> rand.nextInt().toShort()
      else -> throw IllegalStateException()
    }
  }

  fun nextInt(): Int {
    return when (rand.nextInt(5)) {
      0 -> Int.MIN_VALUE
      1 -> Int.MAX_VALUE
      2 -> -0
      3 -> 0
      4 -> rand.nextInt()
      else -> throw IllegalStateException()
    }
  }

  fun nextLong(): Long {
    return when (rand.nextInt(5)) {
      0 -> Long.MIN_VALUE
      1 -> Long.MAX_VALUE
      2 -> -0
      3 -> 0
      4 -> rand.nextLong()
      else -> throw IllegalStateException()
    }
  }

  fun nextDouble(): Double {
    return when (rand.nextInt(8)) {
      0 -> Double.MIN_VALUE
      1 -> Double.MAX_VALUE
      2 -> Double.POSITIVE_INFINITY
      3 -> Double.NEGATIVE_INFINITY
      4 -> Double.NaN
      5 -> -0.0
      6 -> 0.0
      7 -> rand.nextDouble()
      else -> throw IllegalStateException()
    }
  }

  fun nextFloat(): Float {
    return when (rand.nextInt(8)) {
      0 -> Float.MIN_VALUE
      1 -> Float.MAX_VALUE
      2 -> Float.POSITIVE_INFINITY
      3 -> Float.NEGATIVE_INFINITY
      4 -> Float.NaN
      5 -> -0.0f
      6 -> 0.0f
      7 -> rand.nextFloat()
      else -> throw IllegalStateException()
    }
  }

  fun nextString(len: Int): String {
    return (0 until len).map { i -> rand.nextInt(charPool.size) }
        .map(charPool::get)
        .joinToString("")
  }
}
