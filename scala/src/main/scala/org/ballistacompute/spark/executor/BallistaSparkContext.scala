package org.ballistacompute.spark.executor

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.ballistacompute.logical.{LogicalExpr, LogicalPlan, Projection, Scan}

import scala.collection.JavaConverters._

class BallistaSparkContext(spark: SparkSession) {

  /** Translate Ballista logical plan step into a DataFrame transformation */
  def createDataFrame(plan: LogicalPlan, input: Option[DataFrame]): DataFrame = {

    plan match {

      case s: Scan =>
        assert(input.isEmpty)
        val df = spark.read.parquet(s.getName)
        val projection: Seq[String] = s.getProjection().asScala
        df.select(projection.head, projection.tail: _*)

      case p: Projection =>
        val df = input.get
        val expr = p.getExpr.asScala.map(e => createExpression(e, input.get))
        df.select(expr: _*)

      case other =>
        throw new UnsupportedOperationException(s"Ballista logical plan step can not be converted to Spark: $other")
    }

  }

  /** Translate Ballista logical expression into a Spark logical expression */
  def createExpression(e: LogicalExpr, input: DataFrame): Column = {
    e match {
      case c: org.ballistacompute.logical.Column =>
        input.col(c.getName)

      case other =>
        throw new UnsupportedOperationException(s"Ballista logical expression can not be converted to Spark: $other")
    }

  }

}
