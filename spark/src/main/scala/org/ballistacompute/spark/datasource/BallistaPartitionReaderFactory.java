package org.ballistacompute.spark.datasource;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class BallistaPartitionReaderFactory implements PartitionReaderFactory {

  private final TableMeta tableMeta;

  public BallistaPartitionReaderFactory(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    return new BallistaColumnarReader(tableMeta);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
