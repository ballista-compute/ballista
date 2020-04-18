package org.ballistacompute.protobuf

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.logical.*
import java.lang.RuntimeException

class ProtobufDeserializer {

    fun fromProto(node: LogicalPlanNode): LogicalPlan {
        return if (node.hasScan()) {
            val schema = fromProto(node.scan.schema)
            val ds = CsvDataSource(node.scan.path, schema,1024)
            Scan(node.scan.path, ds, node.scan.projectionList.asByteStringList().map { it.toString() })
        } else if (node.hasSelection()) {
            Selection(fromProto(node.input),
                    fromProto(node.selection.expr))
        } else if (node.hasProjection()) {
            Projection(fromProto(node.input),
                    node.projection.exprList.map { fromProto(it) })
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
        } else if (node.hasAggregateExpr()) {
            val aggr = node.aggregateExpr
            val expr = fromProto(aggr.expr)
            return when (aggr.aggrFunction) {
                AggregateFunction.MIN -> Min(expr)
                AggregateFunction.MAX -> Max(expr)
                else -> throw RuntimeException("Failed to parse logical aggregate expression: ${aggr.aggrFunction}")
            }

        } else {
            throw RuntimeException("Failed to parse logical expression: $node")
        }
    }

    fun fromProto(schema: Schema): org.ballistacompute.datatypes.Schema {

        val arrowFields = schema.columnsList.map {

            //TODO add all types
            val dt = when (it.arrowType) {
                ArrowType.UTF8 -> ArrowTypes.StringType
                ArrowType.INT32 -> ArrowTypes.Int32Type
                ArrowType.INT64 -> ArrowTypes.Int64Type
                ArrowType.UINT32 -> ArrowTypes.UInt32Type
                ArrowType.UINT64 -> ArrowTypes.UInt64Type
                ArrowType.HALF_FLOAT -> ArrowTypes.FloatType
                ArrowType.FLOAT -> ArrowTypes.DoubleType
                else -> TODO()
            }

            val fieldType = org.apache.arrow.vector.types.pojo.FieldType(true, dt, null)
            org.apache.arrow.vector.types.pojo.Field(it.name, fieldType, listOf())
        }

        return org.ballistacompute.datatypes.SchemaConverter.fromArrow(org.apache.arrow.vector.types.pojo.Schema(arrowFields))
    }
}