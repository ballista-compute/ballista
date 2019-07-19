package io.andygrove.ballista.spark

import org.apache.spark.sql.SparkSession

object Main {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val sql =
      """
        |SELECT passenger_count, MIN(fare_amount), MAX(fare_amount)
        |FROM tripdata
        |GROUP BY passenger_count
      """.stripMargin

    spark.sql(sql).collect().foreach(println)

  }

}

