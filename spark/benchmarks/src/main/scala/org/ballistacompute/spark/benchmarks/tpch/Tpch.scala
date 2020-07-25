package org.ballistacompute.spark.benchmarks.tpch

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Tpch {

  def main(arg: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    q1(spark)
  }

  def repartition(n: Int) {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[1]")
      .getOrCreate()

      spark.read.parquet("/mnt/tpch/parquet/10/lineitem")
      .repartition(n)
      .write
      .parquet(s"./tmp-$n")
  }

  def adhoc(): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[24]")
      .getOrCreate()

    spark.time {

      spark.read.parquet("/mnt/tpch/parquet/10-24/lineitem").createOrReplaceTempView("lineitem")

      val df = spark.sql(
        """
          | select
          |     l_shipdate, count(*) as n
          | from
          |     lineitem
          | group by
          |     l_shipdate
          | order by l_shipdate desc limit 10
          |""".stripMargin)

      df.show()
      df.explain()
    }

  }

  def q1(spark: SparkSession): Unit = {

    spark.time {

      spark.read.parquet("/mnt/tpch/parquet/10-24/lineitem").createOrReplaceTempView("lineitem")

      val df = spark.sql(
        """
          | select
          |     l_returnflag,
          |     l_linestatus,
          |     sum(l_quantity) as sum_qty,
          |     sum(l_extendedprice) as sum_base_price,
          |     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
          |     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
          |     avg(l_quantity) as avg_qty,
          |     avg(l_extendedprice) as avg_price,
          |     avg(l_discount) as avg_disc,
          |     count(*) as count_order
          | from
          |     lineitem
          | where
          |     l_shipdate < '1998-09-01'
          | group by
          |     l_returnflag,
          |     l_linestatus
          |""".stripMargin)

      df.show()
      df.explain()
    }
  }

  def convertToParquet(csvPath: String, parquetPath: String) {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "|")
      .schema(new StructType(Array(
        StructField("l_orderkey", DataTypes.IntegerType, true),
        StructField("l_partkey", DataTypes.IntegerType, true),
        StructField("l_suppkey", DataTypes.IntegerType, true),
        StructField("l_linenumber", DataTypes.IntegerType, true),
        StructField("l_quantity", DataTypes.DoubleType, true),
        StructField("l_extendedprice", DataTypes.DoubleType, true),
        StructField("l_discount", DataTypes.DoubleType, true),
        StructField("l_tax", DataTypes.DoubleType, true),
        StructField("l_returnflag", DataTypes.StringType, true),
        StructField("l_linestatus", DataTypes.StringType, true),
        StructField("l_shipdate", DataTypes.StringType, true),
        StructField("l_commitdate", DataTypes.StringType, true),
        StructField("l_receiptdate", DataTypes.StringType, true),
        StructField("l_shipinstruct", DataTypes.StringType, true),
        StructField("l_shipmode", DataTypes.StringType, true),
        StructField("l_comment", DataTypes.StringType, true),
      )))
      .csv(csvPath)

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)

    df.printSchema()

  }
}
