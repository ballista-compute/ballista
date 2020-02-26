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
      .option("table", "/home/andy/git/andygrove/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet")
      .option("host", "127.0.0.1")
      .option("port", "50051")
      .load()

    df.printSchema()

//    val query = df
//      .select("a", "b")
//      .filter("c < d")
//
//    query.explain()
//
//    val results = query.collect()

  }
}
