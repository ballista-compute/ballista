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

package org.ballistacompute.protobuf

import java.lang.RuntimeException
import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.logical.*

class ProtobufDeserializer {

  fun fromProto(node: LogicalPlanNode): LogicalPlan {
    return if (node.hasScan()) {
      val schema = fromProto(node.scan.schema)
      val ds = CsvDataSource(node.scan.path, schema, true, 1024)
      Scan(node.scan.path, ds, node.scan.projectionList.asByteStringList().map { it.toString() })
    } else if (node.hasSelection()) {
      Selection(fromProto(node.input), fromProto(node.selection.expr))
    } else if (node.hasProjection()) {
      Projection(fromProto(node.input), node.projection.exprList.map { fromProto(it) })
    } else if (node.hasLimit()) {
      Limit(fromProto(node.input), node.limit.limit)
    } else if (node.hasAggregate()) {
      val input = fromProto(node.input)
      val groupExpr = node.aggregate.groupExprList.map { fromProto(it) }
      val aggrExpr = node.aggregate.aggrExprList.map { fromProto(it) as AggregateExpr }
      Aggregate(input, groupExpr, aggrExpr)
    } else {
      throw RuntimeException("Failed to parse logical operator: $node")
    }
  }

  fun fromProto(node: LogicalExprNode): LogicalExpr {
    return if (node.hasBinaryExpr()) {
      val binaryNode = node.binaryExpr
      val ll = fromProto(binaryNode.l)
      val rr = fromProto(binaryNode.r)
      when (binaryNode.op) {
        "eq" -> Eq(ll, rr)
        "neq" -> Neq(ll, rr)
        "lt" -> Lt(ll, rr)
        "lteq" -> LtEq(ll, rr)
        "gt" -> Gt(ll, rr)
        "gteq" -> GtEq(ll, rr)
        "and" -> And(ll, rr)
        "or" -> Or(ll, rr)
        "add" -> Add(ll, rr)
        "subtract" -> Subtract(ll, rr)
        "multiply" -> Multiply(ll, rr)
        "divide" -> Divide(ll, rr)
        else -> throw RuntimeException("Failed to parse logical binary expression: $node")
      }
    } else if (node.hasColumnIndex) {
      ColumnIndex(node.columnIndex)
    } else if (node.hasColumnName) {
      col(node.columnName)
    } else if (node.hasLiteralString) {
      lit(node.literalString)
    } else if (node.hasLiteralLong) {
      lit(node.literalLong)
    } else if (node.hasLiteralDouble) {
      lit(node.literalDouble)
    } else if (node.hasAggregateExpr()) {
      val aggr = node.aggregateExpr
      val expr = fromProto(aggr.expr)
      return when (aggr.aggrFunction) {
        AggregateFunction.MIN -> Min(expr)
        AggregateFunction.MAX -> Max(expr)
        AggregateFunction.SUM -> Sum(expr)
        AggregateFunction.AVG -> Avg(expr)
        else ->
            throw RuntimeException(
                "Failed to parse logical aggregate expression: ${aggr.aggrFunction}")
      }
    } else {
      throw RuntimeException("Failed to parse logical expression: $node")
    }
  }

  fun fromProto(schema: Schema): org.ballistacompute.datatypes.Schema {

    val arrowFields =
        schema.columnsList.map {

          // TODO add all types
          val dt =
              when (it.arrowTypeValue) {
                ArrowType.UTF8_VALUE -> ArrowTypes.StringType
                ArrowType.INT8_VALUE -> ArrowTypes.Int8Type
                ArrowType.INT16_VALUE -> ArrowTypes.Int16Type
                ArrowType.INT32_VALUE -> ArrowTypes.Int32Type
                ArrowType.INT64_VALUE -> ArrowTypes.Int64Type
                ArrowType.UINT8_VALUE -> ArrowTypes.UInt8Type
                ArrowType.UINT16_VALUE -> ArrowTypes.UInt16Type
                ArrowType.UINT32_VALUE -> ArrowTypes.UInt32Type
                ArrowType.UINT64_VALUE -> ArrowTypes.UInt64Type
                ArrowType.FLOAT_VALUE -> ArrowTypes.FloatType
                ArrowType.DOUBLE_VALUE -> ArrowTypes.DoubleType
                else ->
                    throw IllegalStateException(
                        "Failed to parse Arrow data type enum from protobuf: ${it.arrowTypeValue}")
              }

          val fieldType = org.apache.arrow.vector.types.pojo.FieldType(true, dt, null)
          org.apache.arrow.vector.types.pojo.Field(it.name, fieldType, listOf())
        }

    return org.ballistacompute.datatypes.SchemaConverter
        .fromArrow(org.apache.arrow.vector.types.pojo.Schema(arrowFields))
  }
}
