package org.ballistacompute.spark.benchmarks.tpch

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Tpch {

  def tableSchema(tableName: String) = {
    tableName match {
      case "lineitem" =>
        new StructType(
          Array(
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
          )
        )

      case other => ???
    }
  }

  def query(name: String): String = {
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

      case "q12" =>
        """
          |select
          |                l_shipmode,
          |                sum(case
          |                    when o_orderpriority = '1-URGENT'
          |                        or o_orderpriority = '2-HIGH'
          |                        then 1
          |                    else 0
          |                end) as high_line_count,
          |                sum(case
          |                    when o_orderpriority <> '1-URGENT'
          |                        and o_orderpriority <> '2-HIGH'
          |                        then 1
          |                    else 0
          |                end) as low_line_count
          |            from
          |                lineitem
          |            join
          |                orders
          |            on
          |                l_orderkey = o_orderkey
          |            where
          |                (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')
          |                and l_commitdate < l_receiptdate
          |                and l_shipdate < l_commitdate
          |                and l_receiptdate >= date '1994-01-01'
          |                and l_receiptdate < date '1995-01-01'
          |            group by
          |                l_shipmode
          |            order by
          |                l_shipmode
          |""".stripMargin

      case _ =>
        throw new IllegalArgumentException(s"No query named '$name'")
    }
  }

}
