package io.andygrove.ballista.spark.datasource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DefaultSource extends DataSourceV2 with ReadSupport {

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    new FlightReader(dataSourceOptions)
  }

}
