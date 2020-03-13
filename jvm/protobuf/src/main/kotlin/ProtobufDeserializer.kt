package org.ballistacompute.protobuf

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*

class ProtobufDeserializer {

    fun fromProto(node: LogicalPlanNode): LogicalPlan {
        return if (node.hasFile()) {
            val fileProto = node.file
            val ds = CsvDataSource(fileProto.filename, 1024)
            Scan(fileProto.filename, ds, fileProto.projectionList.asByteStringList().map { it.toString() })
        } else if (node.hasSelection()) {
            Selection(fromProto(node.input),
                    fromProto(node.selection.expr))
        } else if (node.hasProjection()) {
            Projection(fromProto(node.input),
                    node.projection.exprList.map { fromProto(it) })
        } else if (node.hasAggregate()) {
            TODO("aggregate")
        } else {
            TODO(node.toString())
        }
    }

    fun fromProto(node: LogicalExprNode): LogicalExpr {
        return if (node.hasBinaryExpr()) {
            val binaryNode = node.binaryExpr
            when (binaryNode.op) {
                "=" -> Eq(fromProto(binaryNode.l), fromProto(binaryNode.r))
                else -> TODO("binary expr ${binaryNode.op}")
            }
        } else if (node.hasColumnName) {
            col(node.columnName)
        } else if (node.hasLiteralString) {
            lit(node.literalString)
        } else {
            TODO(node.toString())
        }
    }
}