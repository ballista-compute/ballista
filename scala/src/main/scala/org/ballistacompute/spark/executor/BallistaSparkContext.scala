package org.ballistacompute.spark.executor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ballistacompute.logical.{LogicalPlan, Scan}

import scala.collection.JavaConverters._

class BallistaSparkContext(spark: SparkSession) {

  def foo(plan: LogicalPlan): DataFrame = {

    plan match {
      case s: Scan =>
        val df = spark.read.parquet(s.getName)
        val projection: Seq[String] = s.getProjection().asScala
        df.select(projection: _*)

    }

  }

}
