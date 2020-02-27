package io.andygrove.ballista.spark.datasource;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.flight.FlightClient;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class BallistaTable implements Table, SupportsRead {

  private TableMeta tableMeta;

  public BallistaTable(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public String name() {
    return tableMeta.tableName;
  }

  @Override
  public StructType schema() {
    return tableMeta.schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new BallistaScanBuilder(tableMeta);
  }
}
