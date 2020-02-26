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

  private final FlightClient flightClient;

  private final String tableName;

  private final StructType schema;

  public BallistaTable(FlightClient flightClient, String tableName, StructType schema) {
    this.flightClient = flightClient;
    this.tableName = tableName;
    this.schema = schema;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    throw new UnsupportedOperationException();
  }
}
