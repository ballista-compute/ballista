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

        val sparkSchema = ArrowUtils.fromArrowSchema(s.schema().toArrow())

        val df = spark.read
          .format("csv")
          .option("header", "true") //TODO do not hard-code
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

      case l: ballista.Limit =>
        val df = createDataFrame(l.getInput, input)
        df.limit(l.getLimit)

      case a: ballista.Aggregate =>
        val df = createDataFrame(a.getInput, input)
        val groupExpr = a.getGroupExpr.asScala.map(e => createExpression(e, df))
        val dfGrouped = df.groupBy(groupExpr: _*)

        // this assumes simple aggregate expressions of the form aggr_expr(field_expr)
        val aggrExpr = a.getAggregateExpr.asScala.map { aggr =>
          val fieldName = aggr.getExpr.toField(a.getInput).getName
          val aggrFunction = aggr.getName.toLowerCase
          aggrFunction match {
            case "min" => min(col(fieldName))
            case "max" => max(col(fieldName))
            case "sum" => sum(col(fieldName))
            case "avg" => avg(col(fieldName))
          }
        }

        if (aggrExpr.length == 1) {
          dfGrouped.agg(aggrExpr.head)
        } else {
          dfGrouped.agg(aggrExpr.head, aggrExpr.tail: _*)
        }

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
