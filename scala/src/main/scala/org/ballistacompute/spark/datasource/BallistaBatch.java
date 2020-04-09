package org.ballistacompute.spark.datasource;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class BallistaBatch implements Batch {

  private final TableMeta tableMeta;

  public BallistaBatch(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return new InputPartition[] { new BallistaInputPartition() };
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BallistaPartitionReaderFactory(tableMeta);
  }
}
