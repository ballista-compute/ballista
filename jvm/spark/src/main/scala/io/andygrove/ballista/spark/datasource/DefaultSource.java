package io.andygrove.ballista.spark.datasource;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DefaultSource implements TableProvider {

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    options.forEach( (k,v) -> System.out.println(k + "=" + v));
    String tableName = options.get("table");
    return new BallistaTable(tableName);
  }

}
