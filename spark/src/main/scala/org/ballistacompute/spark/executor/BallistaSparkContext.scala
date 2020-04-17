package org.ballistacompute.spark.executor

import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.ballistacompute.{logical => ballista}

import scala.collection.JavaConverters._

class BallistaSparkContext(spark: SparkSession) {

  /** Translate Ballista logical plan step into a DataFrame transformation */
  def createDataFrame(plan: ballista.LogicalPlan, input: Option[DataFrame]): DataFrame = {

    plan match {

      case s: ballista.Scan =>
        assert(input.isEmpty)

        val sparkSchema = ArrowUtils.fromArrowSchema(s.schema())

        val df = spark.read
          .format("csv")
          .schema(sparkSchema)
          .load(s.getPath)

        val projection: Seq[String] = s.getProjection().asScala
        if (projection.isEmpty) {
          df
        } else if (projection.length == 1) {
          df.select(projection.head)
        } else {
          df.select(projection.head, projection.tail: _*)
        }

      case p: ballista.Projection =>
        val df = createDataFrame(p.getInput, input)
        val projectionExpr = p.getExpr.asScala.map(e => createExpression(e, df))
        df.select(projectionExpr: _*)

      case s: ballista.Selection =>
        val df = createDataFrame(s.getInput, input)
        val filterExpr = createExpression(s.getExpr, df)
        df.filter(filterExpr)

      case a: ballista.Aggregate =>
        val df = createDataFrame(a.getInput, input)
        val groupExpr = a.getGroupExpr.asScala.map(e => createExpression(e, df))

        // this assumes simple aggregate expressions of the form aggr_expr(field_expr)
        val aggrMap: Map[String, String]  = a.getAggregateExpr.asScala.map { aggr =>
          val aggrFunction = aggr.getName.toLowerCase
          val fieldName = aggr.getExpr.toField(a.getInput).getName
          fieldName -> aggrFunction
        }.toMap

        df.groupBy(groupExpr: _*).agg(aggrMap)

      case other =>
        throw new UnsupportedOperationException(s"Ballista logical plan step can not be converted to Spark: $other")
    }

  }

  /** Translate Ballista logical expression into a Spark logical expression */
  def createExpression(expr: ballista.LogicalExpr, input: DataFrame): Column = {
    expr match {

      case c: ballista.LiteralDouble => lit(c.getN)
      case c: ballista.LiteralLong => lit(c.getN)
      case c: ballista.LiteralString => lit(c.getStr)

      case c: ballista.Column =>
        input.col(c.getName)

      case b: org.ballistacompute.logical.BinaryExpr =>
        val l = createExpression(b.getL, input)
        val r = createExpression(b.getR, input)
        b match {

          case _: ballista.Add => l.plus(r)
          case _: ballista.Subtract => l.minus(r)
          case _: ballista.Multiply => l.multiply(r)
          case _: ballista.Divide => l.divide(r)

          case _: ballista.Eq => l.equalTo(r)
          case _: ballista.Neq => l.notEqual(r)
          case _: ballista.Gt => l > r
          case _: ballista.GtEq => l >= r
          case _: ballista.Lt => l < r
          case _: ballista.LtEq => l <= r

          case _: ballista.And => l.and(r)
          case _: ballista.Or => l.or(r)

          case other =>
            throw new UnsupportedOperationException(s"Ballista logical binary expression can not be converted to Spark: $other")
        }

      case other =>
          throw new UnsupportedOperationException(s"Ballista logical expression can not be converted to Spark: $other")
      }
  }

}