// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.spark.datasource;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class DefaultSource implements TableProvider {

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {

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

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
