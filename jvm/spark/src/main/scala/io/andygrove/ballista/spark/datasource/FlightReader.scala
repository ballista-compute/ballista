package io.andygrove.ballista.spark.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.JavaConverters._

class FlightReader(options: DataSourceOptions) extends DataSourceReader with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private val tableName = Utils.toScalaOption(options.get(DataSourceOptions.TABLE_KEY))
    .getOrElse(throw new IllegalArgumentException(s"Missing argument '${DataSourceOptions.TABLE_KEY}'"))

  private var projection: StructType = StructType(Array.empty[StructField])

  private var filters: Array[Filter] = Array.empty

  override def readSchema(): StructType = {
    //TODO this needs to be the schema from the table of course and not hard coded
    StructType(Seq(
      StructField("a", DataTypes.ShortType, nullable = true),
      StructField("b", DataTypes.ShortType, nullable = true),
      StructField("c", DataTypes.ShortType, nullable = true),
      StructField("d", DataTypes.ShortType, nullable = true),
      StructField("e", DataTypes.ShortType, nullable = true)
    ))
  }

  override def pruneColumns(structType: StructType): Unit = {
    this.projection = structType
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    this.filters
  }

  override def pushedFilters(): Array[Filter] = filters

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    println(s"Scanning with projection $projection and filter $filters")
    val partition: InputPartition[InternalRow] = new FlightInputPartition()
    List(partition).asJava
  }
}
