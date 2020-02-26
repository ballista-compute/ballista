package io.andygrove.ballista.spark.datasource;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
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

import java.util.concurrent.TimeUnit;

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

    FlightInfo info = client.getInfo(FlightDescriptor.path(tableName));

    Schema arrowSchema = info.getSchema();

    StructType sparkSchema = ArrowUtils.fromArrowSchema(arrowSchema);

    return new BallistaTable(client, tableName, sparkSchema);
  }

}
