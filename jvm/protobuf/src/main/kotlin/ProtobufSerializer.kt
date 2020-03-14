package org.ballistacompute.protobuf

import org.ballistacompute.logical.*

/**
 * Utility to convert between logical plan and protobuf representation.
 */
class ProtobufSerializer {

    /** Convert a logical plan to a protobuf representation */
    fun toProto(plan: LogicalPlan): LogicalPlanNode {
        return when (plan) {
            is Scan -> {
                LogicalPlanNode
                        .newBuilder()
                        .setFile(FileNode.newBuilder()
                                .setFilename(plan.name)
                                //TODO schema
                                .addAllProjection(plan.projection)
                                .build())
                        .build()
            }
            is Projection -> {
                LogicalPlanNode
                        .newBuilder()
                        .setInput(toProto(plan.input))
                        .setProjection(ProjectionNode
                                .newBuilder()
                                .addAllExpr(plan.expr.map { toProto(it) })
                                .build())
                        .build()
            }
            is Selection -> {
                LogicalPlanNode
                        .newBuilder()
                        .setInput(toProto(plan.input))
                        .setSelection(SelectionNode
                                .newBuilder()
                                .setExpr((toProto(plan.expr)))
                                .build())
                        .build()

            }
            else -> TODO(plan.javaClass.name)
        }
    }

    /** Convert a logical expression to a protobuf representation */
    fun toProto(expr: LogicalExpr): LogicalExprNode {
        return when (expr) {
            is Column -> {
                LogicalExprNode.newBuilder()
                        .setHasColumnName(true)
                        .setColumnName(expr.name).build()
            }
            is LiteralString -> {
                LogicalExprNode.newBuilder()
                        .setHasLiteralString(true)
                        .setLiteralString(expr.str).build()
            }
            is Eq -> {
                LogicalExprNode
                        .newBuilder().setBinaryExpr(
                        BinaryExprNode.newBuilder()
                                .setL(toProto(expr.l))
                                .setOp("eq")
                                .setR(toProto(expr.r))
                                .build())
                        .build()
            }
            else -> TODO(expr.javaClass.name)
        }
    }

}