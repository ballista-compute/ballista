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

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*

/** Utility to convert between logical plan and protobuf representation. */
class ProtobufSerializer {

  /** Convert a logical plan to a protobuf representation */
  fun toProto(plan: LogicalPlan): LogicalPlanNode {
    return when (plan) {
      is Scan -> {
        val ds = plan.dataSource
        when (ds) {
          is CsvDataSource -> {
            LogicalPlanNode.newBuilder()
                .setScan(
                    ScanNode.newBuilder()
                        .setPath(plan.path)
                        .addAllProjection(plan.projection)
                        .build())
                .build()
          }
          else -> throw UnsupportedOperationException("Unsupported datasource used in scan")
        }
      }
      is Projection -> {
        LogicalPlanNode.newBuilder()
            .setInput(toProto(plan.input))
            .setProjection(
                ProjectionNode.newBuilder().addAllExpr(plan.expr.map { toProto(it) }).build())
            .build()
      }
      is Selection -> {
        LogicalPlanNode.newBuilder()
            .setInput(toProto(plan.input))
            .setSelection(SelectionNode.newBuilder().setExpr((toProto(plan.expr))).build())
            .build()
      }
      is Limit -> {
        LogicalPlanNode.newBuilder()
            .setInput(toProto(plan.input))
            .setLimit(LimitNode.newBuilder().setLimit(plan.limit).build())
            .build()
      }
      is Aggregate -> {
        LogicalPlanNode.newBuilder()
            .setInput(toProto(plan.input))
            .setAggregate(
                AggregateNode.newBuilder()
                    .addAllGroupExpr(plan.groupExpr.map { toProto(it) })
                    .addAllAggrExpr(plan.aggregateExpr.map { toProto(it) })
                    .build())
            .build()
      }
      else ->
          throw IllegalStateException(
              "Cannot serialize logical operator to protobuf: ${plan.javaClass.name}")
    }
  }

  /** Convert a logical expression to a protobuf representation */
  fun toProto(expr: LogicalExpr): LogicalExprNode {
    return when (expr) {
      is Column -> {
        LogicalExprNode.newBuilder().setHasColumnName(true).setColumnName(expr.name).build()
      }
      is LiteralString -> {
        LogicalExprNode.newBuilder().setHasLiteralString(true).setLiteralString(expr.str).build()
      }
        is LiteralFloat -> {
            LogicalExprNode.newBuilder().setHasLiteralF32(true).setLiteralF32(expr.n).build()
        }
      is LiteralDouble -> {
        LogicalExprNode.newBuilder().setHasLiteralF64(true).setLiteralF64(expr.n).build()
      }
      is LiteralLong -> {
        LogicalExprNode.newBuilder().setHasLiteralI64(true).setLiteralInt(expr.n).build()
      }
      is BooleanBinaryExpr -> {
        val op =
            when (expr) {
              is Eq -> "eq"
              is Neq -> "neq"
              is Lt -> "lt"
              is LtEq -> "lteq"
              is Gt -> "gt"
              is GtEq -> "gteq"
              is And -> "and"
              is Or -> "or"
              else ->
                  throw IllegalStateException(
                      "Cannot serialize logical binary expression to protobuf: ${expr.javaClass.name}")
            }
        LogicalExprNode.newBuilder()
            .setBinaryExpr(
                BinaryExprNode.newBuilder()
                    .setL(toProto(expr.l))
                    .setOp(op)
                    .setR(toProto(expr.r))
                    .build())
            .build()
      }
      else ->
          throw IllegalStateException(
              "Cannot serialize logical expression to protobuf: ${expr.javaClass.name}")
    }
  }
}
