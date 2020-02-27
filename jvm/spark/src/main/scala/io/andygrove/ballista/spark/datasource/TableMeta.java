package io.andygrove.ballista.spark.datasource;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class TableMeta implements Serializable {

  String host;
  int port;
  String tableName;
  StructType schema;

  public TableMeta(String host, int port, String tableName, StructType schema) {
    this.host = host;
    this.port = port;
    this.tableName = tableName;
    this.schema = schema;
  }
}
