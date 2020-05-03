package org.ballistacompute.spark.benchmarks;

object SparkSqlJob {

  def main(arg: Array[String]): Unit = {

    val path = arg.head
    val sql = arg(1)

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    spark.read.parquet(path).createOrReplaceTempView("tripdata")

    val result = spark.sql(sql).collect().map(_.toString()).mkString("\n")

  }

}
