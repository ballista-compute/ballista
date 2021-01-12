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

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use std::pin::Pin;

use crate::serde::decode_protobuf;
use crate::serde::scheduler::Action as BallistaAction;

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use datafusion::execution::context::ExecutionContext;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {}

// impl BallistaFlightService {
//     pub fn new() -> Self {
//         Self {}
//     }
// }

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action = decode_protobuf(&ticket.ticket.to_vec()).map_err(|e| to_tonic_err(&e))?;

        match &action {
            BallistaAction::InteractiveQuery { plan, .. } => {
                // execute with DataFusion for now until distributed execution is in place

                let mut datafusion_ctx = ExecutionContext::new();
                let plan = datafusion_ctx
                    .optimize(&plan)
                    .and_then(|plan| datafusion_ctx.create_physical_plan(&plan))
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;

                let schema = plan.schema().clone();

                //TODO execute all partitions
                let mut stream = plan
                    .execute(0)
                    .await
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                let mut results = vec![];
                while let Some(batch) = stream.next().await {
                    results.push(batch?);
                }

                // first FlightData represents schema
                let mut flights: Vec<Result<FlightData, Status>> =
                    vec![ /*Ok(FlightData::from(&schema))*/ ];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .data
                    .iter()
                    .map(|batch| Ok(FlightData::from(batch)))
                    .collect();

                flights.append(&mut batches);

                let output = futures::stream::iter(flights);
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            _ => Err(Status::invalid_argument("Invalid action")),
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::invalid_argument("get_schema() unimplemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info");
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();
        while let Some(data) = request.next().await {
            let data = data?;
        }
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let _action = decode_protobuf(&action.body.to_vec()).map_err(|e| to_tonic_err(&e))?;
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_tonic_err(e: &crate::error::BallistaError) -> Status {
    Status::internal(format!("{:?}", e))
}
