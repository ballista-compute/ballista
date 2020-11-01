package org.ballistacompute.spark.benchmarks.tpch

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Tpch {

  val LINEITEM_SCHEMA = new StructType(Array(
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
    StructField("l_comment", DataTypes.StringType, true)
  ))

  def query(name: String) : String = {
    name match {
      case "q1" =>
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
          |""".stripMargin

      case _ =>
        throw new IllegalArgumentException(s"No query named '$name'")
    }
  }

}
