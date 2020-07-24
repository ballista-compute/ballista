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

//! Client API for sending requests to executors.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::arrow::datatypes::Schema;
use crate::arrow::flight::flight_data_to_batch;
use crate::arrow::record_batch::RecordBatch;
use crate::error::{ballista_error, BallistaError};
use crate::execution::physical_plan::Action;
use crate::flight::flight_service_client::FlightServiceClient;
use crate::flight::Ticket;
use crate::protobuf;

use prost::Message;

pub async fn execute_action(
    host: &str,
    port: usize,
    action: &Action,
) -> Result<Vec<RecordBatch>, BallistaError> {
    //TODO need to avoid connecting per request

    let addr = format!("http://{}:{}", host, port);
    println!("Connecting to flight server at {}", addr);
    let mut client = FlightServiceClient::connect(addr)
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?;
    println!("connected ok");

    let serialized_action: protobuf::Action = action.try_into()?;
    let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());
    serialized_action
        .encode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

    let request = tonic::Request::new(Ticket { ticket: buf });

    let mut stream = client
        .do_get(request)
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        .into_inner();

    // the schema should be the first message returned, else client should error
    match stream
        .message()
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?
    {
        Some(flight_data) => {
            // convert FlightData to a stream
            let schema = Arc::new(Schema::try_from(&flight_data)?);

            // all the remaining stream messages should be dictionary and record batches
            let mut batches = vec![];
            while let Some(flight_data) = stream
                .message()
                .await
                .map_err(|e| BallistaError::General(format!("{:?}", e)))?
            {
                match flight_data_to_batch(&flight_data, schema.clone())? {
                    Some(batch) => batches.push(batch),
                    _ => {
                        return Err(ballista_error(
                            "Error converting flight data to columnar batch",
                        ))
                    }
                }
            }

            Ok(batches)
        }
        None => Err(ballista_error(
            "Did not receive schema batch from flight server",
        )),
    }
}
