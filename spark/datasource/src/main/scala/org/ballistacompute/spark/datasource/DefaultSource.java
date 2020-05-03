package org.ballistacompute.spark.datasource;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DefaultSource implements TableProvider {

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {

    options.forEach( (k,v) -> System.out.println(k + "=" + v));

    String tableName = options.get("table");
    String host = options.get("host");
    int port = Integer.parseInt(options.get("port"));

    FlightClient client = FlightClient.builder()
        .allocator(new RootAllocator(Long.MAX_VALUE))
        .location(Location.forGrpcInsecure(host, port))
        .build();

//    SchemaResult schemaResult = client.getSchema(FlightDescriptor.path(tableName));
//    Schema arrowSchema = schemaResult.getSchema();

//    FlightInfo info = client.getInfo(FlightDescriptor.path(tableName));
//    Schema arrowSchema = info.getSchema();

    Ticket ticket = new Ticket("SELECT id FROM alltypes_plain".getBytes());
    FlightStream stream = client.getStream(ticket);
    Schema arrowSchema = stream.getSchema();

    StructType sparkSchema = ArrowUtils.fromArrowSchema(arrowSchema);

    return new BallistaTable(new TableMeta(host, port, tableName, sparkSchema));
  }

}
