package io.andygrove.ballista.spark

import org.junit.{Ignore, Test}
import org.apache.spark.sql.SparkSession

class DataSourceTest {

  @Test
  def testSomething() {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("io.andygrove.ballista.spark.datasource")
      .option("table", "alltypes_plain")
      .option("host", "localhost")
      .option("port", "50001")
      .load()

    val query = df
      .select("a", "b")
      .filter("c < d")

    query.explain()

    val results = query.collect()

  }
}
