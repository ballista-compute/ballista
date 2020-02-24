package io.andygrove.ballista.spark.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class FlightInputPartition extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new FlightPartitionReader
}
