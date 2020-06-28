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

package org.ballistacompute.client

import java.util.concurrent.TimeUnit
import org.apache.arrow.flight.CallOptions
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.Ticket
import org.apache.arrow.memory.RootAllocator
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.protobuf.ProtobufSerializer

/** Connection to a Ballista executor. */
class Client(val host: String, val port: Int) {

  var client =
      FlightClient.builder()
          .allocator(RootAllocator(Long.MAX_VALUE))
          .location(Location.forGrpcInsecure(host, port))
          .build()

  var callOptions = CallOptions.timeout(600, TimeUnit.SECONDS)

  fun execute(plan: LogicalPlan) {

    val protoBuf = ProtobufSerializer().toProto(plan)

    var ticket = Ticket(protoBuf.toByteArray())

    var stream = client.getStream(ticket, callOptions)
  }
}
