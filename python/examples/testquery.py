#!/usr/bin/env python3
import ballista
import typing 
from ballista import Field
import pyarrow 




import numpy as np 
data_path = "../../benchmarks/tpch/data"
query_path = "../../benchmarks/tpch/queries"
tables = [ "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]


def get_schema(table_name: str)->ballista.Schema:
    fields: typing.List[ballista.Field]
    if table_name == "part":
        fields = [Field("p_partkey","i32", False),
            Field("p_name", "utf8",False),
            Field("p_mfgr", "utf8", False),
            Field("p_brand", "utf8", False),
            Field("p_type", "utf8",False),
            Field("p_size", "i32",False),
            Field("p_container", "utf8",False),
            Field("p_retailprice", "f64",False),
            Field("p_comment", "utf8",False),
        ]
    elif table_name == "supplier":
        fields = [
            Field("s_suppkey", "i32", False),
            Field("s_name", "utf8", False),
            Field("s_address", "utf8", False), 
            Field("s_nationkey", "i32", False),
            Field("s_phone", "utf8", False),
            Field("s_acctbal", "f64", False),
            Field("s_comment", "utf8", False),
        ]
    elif table_name == "partsupp":
        fields = [
            Field("ps_partkey", "i32", False),
            Field("ps_suppkey", "i32", False),
            Field("ps_availqty", "i32", False),
            Field("ps_supplycost", "f64", False),
            Field("ps_comment", "utf8", False),
        ]
    elif table_name == "customer":
        fields = [
            Field("c_custkey","i32", False),
            Field("c_name","utf8", False),
            Field("c_address","utf8", False),
            Field("c_nationkey","i32", False),
            Field("c_phone","utf8", False),
            Field("c_acctbal","f64", False),
            Field("c_mktsegment","utf8", False),
            Field("c_comment","utf8", False),
        ]
    elif table_name == "orders":
        fields = [
            Field("o_orderkey", "i32", False),
            Field("o_custkey", "i32", False),
            Field("o_orderstatus", "str", False),
            Field("o_totalprice", "f64", False),
            Field("o_orderdate", "day32", False),
            Field("o_orderpriority", "str", False),
            Field("o_clerk", "str", False),
            Field("o_shippriority", "i32", False),
            Field("o_comment", "i32", False),
        ]
    elif table_name == "lineitem":
        fields = [
            Field("l_orderkey", "i32", False),
            Field("l_partkey", "i32", False),
            Field("l_suppkey", "i32", False),
            Field("l_linenumber", "i32", False),
            Field("l_quantity", "f64", False),
            Field("l_extendedprice", "f64", False),
            Field("l_discount", "f64", False),
            Field("l_tax", "f64", False),
            Field("l_returnflag", "utf8", False),
            Field("l_linestatus", "utf8", False),
            Field("l_shipdate", "day32", False),
            Field("l_commitdate", "day32", False),
            Field("l_receiptdate", "day32", False),
            Field("l_shipinstruct", "utf8", False),
            Field("l_shipmode", "utf8", False),
            Field("l_comment", "utf8", False),
        ]
    elif table_name == "nation":
        fields = [
            Field("n_nationkey", "i32"),
            Field("n_name","utf8"),
            Field("n_region_key","i32"),
            Field("n_comment", "utf8")
        ]
    elif table_name == "region":
        fields = [
            Field("r_regionkey","i32"),
            Field("r_name", "utf8"),
            Field("r_comment","utf8")
        ]

    if fields is None:
        raise Exception(f"Could not find a schema for {table_name}")
    schema = ballista.Schema(*fields)
    return schema

def get_query_sql(query_no: int)->str:
    if query_no > 0 and query_no < 23 and query_no != 15:
        filename = f"{query_path}/q{query_no}.sql"
        with open(filename, 'r') as f:
            sql = f.read()
        return sql
    else:
        raise Exception(f"The query number {query_no} is invalid")


def register_tables(ctx: ballista.BallistaContext)->None:
    for table in tables:
        path = f"{data_path}/{table}.tbl"
        schema = get_schema(table)
        ctx.register_csv(table,path,  file_extension=".tbl", schema = schema, delimiter='|', has_header = False)


def setup_context(host:str = "localhost", port: int=50051)->ballista.BallistaContext:
    ctx = ballista.BallistaContext(host=host, port=port, **{"batch.size": "5000"})
    register_tables(ctx)
    return ctx


query_no = 1 
def main()->None:
    ctx = setup_context()
    sql_str = get_query_sql(query_no)
    print(sql_str)
    df = ctx.sql(sql_str)
    res = df.collect()
    print(res)
    c = ballista.case(1)




if __name__ == "__main__":
    main()