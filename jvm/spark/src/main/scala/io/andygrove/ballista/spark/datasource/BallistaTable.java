package io.andygrove.ballista.spark.datasource;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class BallistaTable implements Table, SupportsRead {

  private String tableName;

  public BallistaTable(String table) {
    this.tableName = table;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public StructType schema() {
    throw new UnsupportedOperationException();
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
