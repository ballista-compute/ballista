package org.ballistacompute.spark.datasource;

object Utils {

  def toScalaOption[T](o: Optional[T]): Option[T] = {
    if (o.isPresent) {
      Option(o.get())
    } else {
      None
    }
  }

}
