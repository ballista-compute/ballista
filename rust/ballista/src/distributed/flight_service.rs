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

//! Implementation of the Apache Arrow Flight protocol that delegates to an executor instance.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatch;
use crate::distributed::scheduler::{self, create_job, create_physical_plan, ensure_requirements};
use crate::serde::decode_protobuf;
use flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};

use crate::error::{ballista_error, BallistaError};
use crate::execution::physical_plan::ExecutionContext;

use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub struct Results {
    schema: Schema,
    data: Vec<RecordBatch>,
}

#[derive(Clone, Default)]
pub struct FlightServiceImpl {
    results: Arc<Mutex<HashMap<String, Results>>>,
}

impl FlightServiceImpl {
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(HashMap::new())),
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

        // let results = execute_action(&action)
        //     .await
        //     .map_err(|e| to_tonic_err(&e.into()))?;
        //
        // let mut flights: Vec<Result<FlightData, Status>> =
        //     vec![Ok(FlightData::from(&results.schema))];
        //
        // let mut batches: Vec<Result<FlightData, Status>> = results
        //     .data
        //     .iter()
        //     .map(|batch| {
        //         println!("batch schema: {:?}", batch.schema());
        //
        //         Ok(FlightData::from(batch))
        //     })
        //     .collect();
        //
        // flights.append(&mut batches);
        //
        // let output = futures::stream::iter(flights);
        // Ok(Response::new(Box::pin(output) as Self::DoGetStream))

        unimplemented!()
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        println!("get_schema()");

        let request = request.into_inner();
        let uuid = &request.path[0];

        match self.results.lock().unwrap().get(uuid) {
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
            scheduler::Action::Collect { plan: logical_plan } => {
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

        let action = decode_protobuf(&action.body.to_vec()).map_err(|e| to_tonic_err(&e))?;

        // let results = execute_action(&action)
        //     .await
        //     .map_err(|e| to_tonic_err(&e.into()))?;
        //
        // let key = "tbd"; // generate uuid here
        //
        // self.results.lock().unwrap().insert(key.to_owned(), results);
        //
        // let result = vec![Ok(flight::Result {
        //     body: key.as_bytes().to_vec(),
        // })];
        //
        // let output = futures::stream::iter(result);
        // Ok(Response::new(Box::pin(output) as Self::DoActionStream))

        unimplemented!()
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

async fn execute_action(ctx: Arc<dyn ExecutionContext>, action: &scheduler::Action) -> Result<Results, BallistaError> {
    match &action {
        scheduler::Action::ExecuteTask { task } => {
            println!("Executing task {:?}", task);
            let stream = task.plan.as_execution_plan().execute(ctx, task.partition_id)?;

            let mut results = vec![];
            while let Some(batch) = stream.next().await? {
                results.push(batch);
            }

            // TODO return task done message

            unimplemented!()
        }
        scheduler::Action::Collect { plan: logical_plan } => {
            println!("Logical plan: {:?}", logical_plan);

            // // create local execution context
            // let ctx = ExecutionContext::new();
            //
            // // create the query plan
            // let optimized_plan = ctx.optimize(&logical_plan)?;
            //
            // println!("Optimized Plan: {:?}", optimized_plan);
            //
            // let batch_size = 1024 * 1024;
            // let physical_plan = ctx.create_physical_plan(&optimized_plan, batch_size)?;
            //
            // // execute the query
            // let results = ctx.collect(physical_plan.as_ref())?;
            //
            // println!("Executed query");
            //
            // // add an initial FlightData message that sends schema
            // let schema = physical_plan.schema();
            // println!("physical plan schema: {:?}", &schema);
            //
            // Ok(Results {
            //     schema: schema.as_ref().clone(),
            //     data: results,
            // })

            unimplemented!()
        }
        other => Err(ballista_error(&format!(
            "Invalid Ballista action: {:?}",
            other
        ))),
    }
}

fn to_tonic_err(e: &crate::error::BallistaError) -> Status {
    Status::internal(format!("{:?}", e))
}
