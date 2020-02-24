package io.andygrove.ballista.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Example {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.parquet("input.parquet").as[Input]

    val df2 = df.mapPartitions(it => {
      val exchange = new FlightExchange
      exchange.process(it)
    })

    df2.explain()
    val results = df2.collect()

    results.foreach(println)
  }

}

case class Input(a: Int)

case class Output(a: Int)

class FlightExchange {

  def process(it: Iterator[Input]): Iterator[Output] = {
    ???
  }

}
