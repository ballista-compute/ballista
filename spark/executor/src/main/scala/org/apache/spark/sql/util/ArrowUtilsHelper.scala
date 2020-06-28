package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

object ArrowUtilsAccessor {

  def fromArrowSchema(schema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(schema)
  }

}
