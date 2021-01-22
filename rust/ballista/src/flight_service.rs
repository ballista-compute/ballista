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
use std::sync::Arc;

use crate::executor::BallistaExecutor;
use crate::serde::decode_protobuf;
use crate::serde::scheduler::Action as BallistaAction;
use crate::utils::write_stream_to_disk;

use arrow::error::ArrowError;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::collect;
use futures::{Stream, StreamExt};
use log::{debug, info};
use tempfile::TempDir;
use tonic::{Request, Response, Status, Streaming};

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {
    executor: Arc<BallistaExecutor>,
}

impl BallistaFlightService {
    pub fn new(executor: Arc<BallistaExecutor>) -> Self {
        Self { executor }
    }
}

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
        info!("Received do_get request");

        let action = decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::InteractiveQuery { plan, .. } => {
                info!("Running interactive query");
                debug!("Logical plan: {:?}", plan);
                // execute with DataFusion for now until distributed execution is in place
                let ctx = ExecutionContext::new();

                // create the query plan
                let plan = ctx
                    .optimize(&plan)
                    .and_then(|plan| {
                        debug!("Optimized logical plan: {:?}", plan);
                        ctx.create_physical_plan(&plan)
                    })
                    .map_err(|e| from_datafusion_err(&e))?;

                debug!("Physical plan: {:?}", plan);
                // execute the query
                let results = collect(plan.clone())
                    .await
                    .map_err(|e| from_datafusion_err(&e))?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }
                debug!("Received {} record batches", results.len());

                // add an initial FlightData message that sends schema
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let schema = plan.schema();
                let schema_flight_data =
                    arrow_flight::utils::flight_data_from_arrow_schema(schema.as_ref(), &options);

                let mut flights: Vec<Result<FlightData, Status>> = vec![Ok(schema_flight_data)];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .iter()
                    .flat_map(|batch| {
                        let (flight_dictionaries, flight_batch) =
                            arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);
                        flight_dictionaries
                            .into_iter()
                            .chain(std::iter::once(flight_batch))
                            .map(Ok)
                    })
                    .collect();

                // append batch vector to schema vector, so that the first message sent is the schema
                flights.append(&mut batches);

                let output = futures::stream::iter(flights);

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            BallistaAction::ExecuteQueryStage(stage) => {
                // TODO this is work-in-progress / experimental

                // the goal here is to execute a partial query - the query will be send to
                // multiple executors, with each executor executing a number of partitions
                // and the results will be persisted to disk to reduce memory pressure and
                // allow the results to be streamed to other executors in future query stages

                //TODO should not be a temp dir but a configured dir so that the user has control
                // over which drive to use (e.g. choosing NVMe or RAID)
                let work_dir = TempDir::new()?;
                let path = work_dir.path().to_str().unwrap();

                // execute the query partition
                let mut stream = stage
                    .plan
                    .execute(stage.partition_id)
                    .await
                    .map_err(|e| from_datafusion_err(&e))?;

                // stream results to disk
                write_stream_to_disk(&mut stream, &path)
                    .await
                    .map_err(|e| from_arrow_err(&e))?;

                //TODO return summary of the query stage execution
                Err(Status::unimplemented("ExecuteQueryStage"))
            }
            BallistaAction::FetchShuffle(_) => {
                // once query stage execution is implemented, it will be necessary to implement
                // this part so that the results can be collected, either by another query stage
                // or by the client fetching the results

                Err(Status::unimplemented("FetchShuffle"))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();
        while let Some(data) = request.next().await {
            let _data = data?;
        }
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let _action = decode_protobuf(&action.body.to_vec()).map_err(|e| from_ballista_err(&e))?;
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

fn from_ballista_err(e: &crate::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}

fn from_datafusion_err(e: &DataFusionError) -> Status {
    Status::internal(format!("DataFusion Error: {:?}", e))
}
