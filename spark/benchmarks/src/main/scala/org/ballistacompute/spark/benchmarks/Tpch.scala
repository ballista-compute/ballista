package org.ballistacompute.spark.benchmarks

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Tpch {

  def main(arg: Array[String]): Unit = {
    convertToParquet("/mnt/tpch/10/lineitem.tbl", "./tmp")
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
