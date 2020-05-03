package org.ballistacompute.spark.datasource;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

public class BallistaScanBuilder implements ScanBuilder {

  TableMeta tableMeta;

  public BallistaScanBuilder(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public Scan build() {
    return new BallistaScan(tableMeta);
  }
}
