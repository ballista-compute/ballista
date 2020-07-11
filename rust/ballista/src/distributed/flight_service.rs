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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::distributed::executor::{Executor, ShufflePartition};
use crate::execution::physical_plan;
use crate::execution::scheduler::{create_job, create_physical_plan, ensure_requirements};
use crate::serde::decode_protobuf;

use crate::flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};

use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct FlightServiceImpl {
    /// Ballista executor implementation
    executor: Arc<dyn Executor>,
    /// Results cache
    results_cache: Arc<Mutex<HashMap<String, ShufflePartition>>>,
}

impl FlightServiceImpl {
    pub fn new(executor: Arc<dyn Executor>) -> Self {
        Self {
            executor,
            results_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action = decode_protobuf(&ticket.ticket.to_vec()).map_err(|e| to_tonic_err(&e))?;

        println!("do_get: {:?}", action);
        match &action {
            physical_plan::Action::Execute(task) => {
                self.executor
                    .do_task(task)
                    .await
                    .map_err(|e| to_tonic_err(&e))?;
                unimplemented!()
            }
            physical_plan::Action::Collect(shuffle_id) => {
                self.executor
                    .collect(shuffle_id)
                    .map_err(|e| to_tonic_err(&e))?;
                unimplemented!()
            }
            physical_plan::Action::InteractiveQuery { plan } => {
                let results = self
                    .executor
                    .execute_query(plan)
                    .await
                    .map_err(|e| to_tonic_err(&e))?;

                let mut flights: Vec<Result<FlightData, Status>> =
                    vec![Ok(FlightData::from(&results.schema))];

                let mut batches: Vec<Result<FlightData, Status>> = results
                    .data
                    .iter()
                    .map(|batch| {
                        println!("batch schema: {:?}", batch.schema());

                        Ok(FlightData::from(batch))
                    })
                    .collect();

                flights.append(&mut batches);

                let output = futures::stream::iter(flights);
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            _ => unimplemented!(),
        }
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        println!("get_schema()");

        let request = request.into_inner();
        let uuid = &request.path[0];

        match self.results_cache.lock().unwrap().get(uuid) {
            Some(results) => Ok(Response::new(SchemaResult::from(&results.schema))),
            _ => Err(Status::not_found("Invalid uuid")),
        }
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info");

        let request = request.into_inner();

        let action = decode_protobuf(&request.cmd.to_vec()).map_err(|e| to_tonic_err(&e))?;

        match &action {
            physical_plan::Action::InteractiveQuery { plan: logical_plan } => {
                println!("Logical plan: {:?}", logical_plan);

                let plan = create_physical_plan(&logical_plan).map_err(|e| to_tonic_err(&e))?;
                println!("Physical plan: {:?}", plan);

                let plan = ensure_requirements(&plan).map_err(|e| to_tonic_err(&e))?;
                println!("Optimized physical plan: {:?}", plan);

                let job = create_job(plan).map_err(|e| to_tonic_err(&e))?;
                job.explain();

                // TODO execute the DAG by serializing stages to protobuf and allocating
                // tasks (partitions) to executors in the cluster

                Err(Status::invalid_argument("not implemented yet"))

                //     let job = create_job(logical_plan).map_err(|e| to_tonic_err(&e))?;
                //     println!("Job: {:?}", job);
                //
                //     //TODO execute stages
                //
                //     let uuid = "tbd";
                //
                //     match self.results.lock().unwrap().get(uuid) {
                //         Some(results) => {
                //             let schema_bytes = schema_to_bytes(&results.schema);
                //
                //             Ok(Response::new(FlightInfo {
                //                 schema: schema_bytes,
                //                 endpoint: vec![],
                //                 flight_descriptor: None,
                //                 total_bytes: -1,
                //                 total_records: -1,
                //             }))
                //         }
                //         _ => Err(Status::not_found("Invalid uuid")),
                //     }
            }
            _ => Err(Status::invalid_argument("Invalid action")),
        }
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
        println!("do_put()");

        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let data = data?;
            println!("do_put() received data: {:?}", data);
        }

        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        println!("do_action() type={}", action.r#type);

        let _action = decode_protobuf(&action.body.to_vec()).map_err(|e| to_tonic_err(&e))?;

        unimplemented!()

        // let results = execute_action(&action)?;
        //
        // let key = "tbd"; // generate uuid here
        //
        // self.results_cache
        //     .lock()
        //     .unwrap()
        //     .insert(key.to_owned(), results);
        //
        // let result = vec![Ok(flight::Result {
        //     body: key.as_bytes().to_vec(),
        // })];
        //
        // let output = futures::stream::iter(result);
        // Ok(Response::new(Box::pin(output) as Self::DoActionStream))
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

//TODO this is private in arrow so copied here
// fn schema_to_bytes(schema: &Schema) -> Vec<u8> {
//     let mut fbb = FlatBufferBuilder::new();
//     let schema = {
//         let fb = ipc::convert::schema_to_fb_offset(&mut fbb, schema);
//         fb.as_union_value()
//     };
//
//     let mut message = ipc::MessageBuilder::new(&mut fbb);
//     message.add_version(ipc::MetadataVersion::V4);
//     message.add_header_type(ipc::MessageHeader::Schema);
//     message.add_bodyLength(0);
//     message.add_header(schema);
//     // TODO: custom metadata
//     let data = message.finish();
//     fbb.finish(data, None);
//
//     let data = fbb.finished_data();
//     data.to_vec()
// }

fn to_tonic_err(e: &crate::error::BallistaError) -> Status {
    Status::internal(format!("{:?}", e))
}
