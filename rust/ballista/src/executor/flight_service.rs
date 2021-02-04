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
use std::time::Instant;

use crate::executor::BallistaExecutor;
use crate::scheduler::planner::pretty_print;
use crate::serde::decode_protobuf;
use crate::serde::scheduler::Action as BallistaAction;
use crate::utils::write_stream_to_disk;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use datafusion::error::DataFusionError;
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
                let results = self
                    .executor
                    .execute_logical_plan(&plan)
                    .await
                    .map_err(|e| from_ballista_err(&e))?;

                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }
                debug!("Received {} record batches", results.len());

                let df_schema = plan.schema();
                let arrow_schema: Schema = df_schema.clone().as_ref().clone().into();

                let flights = create_flight_data(Arc::new(arrow_schema), results);
                let output = futures::stream::iter(flights);
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            BallistaAction::ExecutePartition(partition) => {
                pretty_print(partition.plan.clone(), 0);

                let work_dir = TempDir::new()?;
                let mut path = work_dir.into_path();
                path.push(&format!("{}", partition.job_uuid));
                path.push(&format!("{}", partition.stage_id));
                path.push(&format!("{}", partition.partition_id));
                std::fs::create_dir_all(&path)?;

                path.push("data.arrow");
                let path = path.to_str().unwrap();
                info!("Writing results to {}", path);

                let now = Instant::now();

                // execute the query partition
                let mut stream = partition
                    .plan
                    .execute(partition.partition_id)
                    .await
                    .map_err(|e| from_datafusion_err(&e))?;

                // stream results to disk
                write_stream_to_disk(&mut stream, &path)
                    .await
                    .map_err(|e| from_arrow_err(&e))?;

                // build result set with summary of the partition execution status
                let mut c0 = StringBuilder::new(1);
                c0.append_value(&path).unwrap();
                let path: ArrayRef = Arc::new(c0.finish());

                let schema = Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, false)]));

                let results = vec![RecordBatch::try_new(schema.clone(), vec![path]).unwrap()];
                let flights = create_flight_data(schema, results);
                let output = futures::stream::iter(flights);

                info!("Executed partition in {} seconds", now.elapsed().as_secs());

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            BallistaAction::FetchPartition(partition_id) => {
                // fetch a partition that was previously executed by this executor
                debug!("FetchPartition {:?}", partition_id);
                Err(Status::unimplemented("FetchPartition"))
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

/// Convert a result set into flight data
fn create_flight_data(
    schema: SchemaRef,
    results: Vec<RecordBatch>,
) -> Vec<Result<FlightData, Status>> {
    // add an initial FlightData message that sends schema
    let options = arrow::ipc::writer::IpcWriteOptions::default();
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

    flights
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
