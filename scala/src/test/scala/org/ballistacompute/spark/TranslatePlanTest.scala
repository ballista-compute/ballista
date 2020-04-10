package org.ballistacompute.spark

import org.apache.spark.sql.SparkSession
import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.spark.executor.BallistaSparkContext
import org.ballistacompute.{logical => ballista}
import org.junit.{Ignore, Test}

import scala.collection.JavaConverters._

class TranslatePlanTest {

  @Test
  def testSomething() {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val filename = "/mnt/nyctaxi/csv/yellow/2019/yellow_tripdata_2019-01.csv"

    val projection = List[String]().asJava

    val plan = new ballista.DataFrameImpl(new ballista.Scan(filename, new CsvDataSource(filename, 1024), projection))
      .filter(new ballista.Eq(new ballista.Column("passenger_count"), new ballista.LiteralLong(1)))
      .project(List[ballista.LogicalExpr](new ballista.Column("fare_amt")).asJava)
      .logicalPlan()

    println(plan.pretty())

    val ctx = new BallistaSparkContext(spark)
    val df = ctx.createDataFrame(plan, None)
    df.explain()
  }
}
