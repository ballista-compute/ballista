package org.ballistacompute.spark.benchmarks

import org.apache.spark.sql.{DataFrame, SparkSession}
;

object SparkUtils {

  def loadParquet(spark: SparkSession, name: String, path: String) {
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  def createResponse(df: DataFrame): String = {
    //TODO create streaming response instead of loading result set into memory
    //TODO json format
    df.collect().map(_.toString()).mkString("\n")
  }

}
