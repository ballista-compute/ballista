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

use std::fs::File;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::error::BallistaError;
use crate::executor::BallistaExecutor;
use crate::memory_stream::MemoryStream;
use crate::serde::decode_protobuf;
use crate::serde::scheduler::Action as BallistaAction;
use crate::utils::{self, format_plan};

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use crossbeam::channel::{bounded, Receiver, RecvError, Sender};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use log::{debug, info};
use tokio::task;
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
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        info!("Received do_get request");

        let action = decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::ExecutePartition(partition) => {
                info!(
                    "ExecutePartition: job={}, stage={}, partition={}\n{}",
                    partition.job_uuid,
                    partition.stage_id,
                    partition.partition_id,
                    format_plan(partition.plan.as_ref(), 0).map_err(|e| from_ballista_err(&e))?
                );

                let mut path = PathBuf::from(&self.executor.config.work_dir);
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
                let info = utils::write_stream_to_disk(&mut stream, &path)
                    .await
                    .map_err(|e| from_ballista_err(&e))?;

                info!(
                    "Executed partition in {} seconds. Statistics: {:?}",
                    now.elapsed().as_secs(),
                    info
                );

                // build result set with summary of the partition execution status
                let mut c0 = StringBuilder::new(1);
                c0.append_value(&path).unwrap();
                let path: ArrayRef = Arc::new(c0.finish());

                let schema = Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, false)]));

                let results = vec![RecordBatch::try_new(schema.clone(), vec![path]).unwrap()];
                // add an initial FlightData message that sends schema
                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let schema_flight_data =
                    arrow_flight::utils::flight_data_from_arrow_schema(schema.as_ref(), &options);

                let mut flights: Vec<Result<FlightData, Status>> = vec![Ok(schema_flight_data)];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .iter()
                    .flat_map(|batch| create_flight_iter(batch, &options))
                    .collect();

                // append batch vector to schema vector, so that the first message sent is the schema
                flights.append(&mut batches);
                let output = futures::stream::iter(flights);

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            BallistaAction::FetchPartition(partition_id) => {
                // fetch a partition that was previously executed by this executor
                info!("FetchPartition {:?}", partition_id);

                let mut path = PathBuf::from(&self.executor.config.work_dir);
                path.push(&format!("{}", partition_id.job_uuid));
                path.push(&format!("{}", partition_id.stage_id));
                path.push(&format!("{}", partition_id.partition_id));
                path.push("data.arrow");
                let path = path.to_str().unwrap();

                let options = arrow::ipc::writer::IpcWriteOptions::default();

                info!("FetchPartition {:?} reading {}", partition_id, path);
                let file = File::open(&path)
                    .map_err(|e| {
                        BallistaError::General(format!(
                            "Failed to open partition file at {}: {:?}",
                            path, e
                        ))
                    })
                    .map_err(|e| from_ballista_err(&e))?;
                let reader = FileReader::try_new(file).map_err(|e| from_arrow_err(&e))?;
                let schema = reader.schema();
                let schema_flight_data = arrow_flight::utils::flight_data_from_arrow_schema(
                    schema.as_ref(),
                    &options,
                );

                let (response_tx, response_rx): (
                    Sender<Option<Result<FlightData, Status>>>,
                    Receiver<Option<Result<FlightData, Status>>>,
                ) = bounded(2);

                task::spawn_blocking(move || {
                    for batch in reader {
                        let mut batch_flight_data: Vec<_> = batch
                            .map(|b| create_flight_iter(&b, &options).collect())
                            .map_err(|e| from_arrow_err(&e)).unwrap(); //TODO err

                        for batch in &batch_flight_data {
                            response_tx.send(Some(batch.clone())).unwrap();
                        }
                    }
                    response_tx.send(None).unwrap();
                });

                let flights = Box::pin(FlightDataStream {
                    schema: schema.clone(),
                    response_rx
                });

                Ok(Response::new(flights) as Self::DoGetStream)
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

/// Convert a single RecordBatch into an iterator of FlightData (containing
/// dictionaries and batches)
fn create_flight_iter(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> Box<dyn Iterator<Item = Result<FlightData, Status>>> {
    let (flight_dictionaries, flight_batch) =
        arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);
    Box::new(
        flight_dictionaries
            .into_iter()
            .chain(std::iter::once(flight_batch))
            .map(Ok),
    )
}


struct FlightDataStream {
    schema: SchemaRef,
    response_rx: Receiver<Option<Result<FlightData, Status>>>,
}

impl Stream for FlightDataStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.response_rx.recv() {
            Ok(batch) => Poll::Ready(batch),
            // RecvError means receiver has exited and closed the channel
            Err(RecvError) => Poll::Ready(None),
        }
    }
}

// impl RecordBatchStream for FooStream {
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
// }

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

fn from_ballista_err(e: &crate::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}

fn from_datafusion_err(e: &DataFusionError) -> Status {
    Status::internal(format!("DataFusion Error: {:?}", e))
}
