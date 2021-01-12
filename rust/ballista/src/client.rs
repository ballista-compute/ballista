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

use crate::error::{ballista_error, BallistaError, Result};
use crate::serde::protobuf::{self};
use crate::serde::scheduler::Action;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use datafusion::logical_plan::LogicalPlan;
use prost::Message;
use std::collections::HashMap;

/// Client for sending actions to Ballista executors.
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port
    pub async fn try_new(host: &str, port: usize) -> Result<Self> {
        let addr = format!("http://{}:{}", host, port);
        let flight_client = FlightServiceClient::connect(addr)
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

        Ok(Self { flight_client })
    }

    /// Execute a logical query plan and retrieve the results
    pub async fn execute_query(&mut self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let action = Action::InteractiveQuery {
            plan: plan.to_owned(),
            settings: HashMap::new(),
        };

        let serialized_action: protobuf::Action = action.to_owned().try_into()?;
        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());
        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self
            .flight_client
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
                    let batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])?;
                    batches.push(batch);
                }
                Ok(batches)
            }
            None => Err(ballista_error(
                "Did not receive schema batch from flight server",
            )),
        }
    }
}
