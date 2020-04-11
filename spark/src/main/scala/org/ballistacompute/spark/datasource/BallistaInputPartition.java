package org.ballistacompute.spark.datasource;

import org.apache.spark.sql.connector.read.InputPartition;

public class BallistaInputPartition implements InputPartition {

  @Override
  public String[] preferredLocations() {
    return new String[] { "bar" };
  }
}
