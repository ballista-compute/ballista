package io.andygrove.ballista.spark.datasource;

import org.apache.arrow.flight.FlightClient;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

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
