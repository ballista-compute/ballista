use std::pin::Pin;

use ballista::logicalplan::translate_plan;
use ballista::serde::decode_protobuf;
use ballista::{plan, BALLISTA_VERSION};

use ballista::datafusion::execution::context::ExecutionContext;

use flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        match decode_protobuf(&ticket.ticket.to_vec()) {
            Ok(action) => {
                println!("do_get: {:?}", action);

                match &action {
                    plan::Action::Collect { plan: logical_plan } => {
                        println!("Logical plan: {:?}", logical_plan);

                        // create local execution context
                        let mut ctx = ExecutionContext::new();

                        let datafusion_plan =
                            translate_plan(&mut ctx, logical_plan).map_err(|e| to_tonic_err(&e))?;

                        // create the query plan
                        let optimized_plan = ctx
                            .optimize(&datafusion_plan)
                            .map_err(|e| to_tonic_err(&e))?;

                        println!("Optimized Plan: {:?}", optimized_plan);

                        let physical_plan = ctx
                            .create_physical_plan(&optimized_plan, 1024 * 1024)
                            .map_err(|e| to_tonic_err(&e))?;

                        // execute the query
                        let results = ctx
                            .collect(physical_plan.as_ref())
                            .map_err(|e| to_tonic_err(&e))?;

                        println!("Executed query");

                        if results.is_empty() {
                            return Err(Status::internal("There were no results from ticket"));
                        }

                        // add an initial FlightData message that sends schema
                        let schema = physical_plan.schema();
                        let mut flights: Vec<Result<FlightData, Status>> =
                            vec![Ok(FlightData::from(schema.as_ref()))];

                        let mut batches: Vec<Result<FlightData, Status>> = results
                            .iter()
                            .map(|batch| Ok(FlightData::from(batch)))
                            .collect();

                        // append batch vector to schema vector, so that the first message sent is the schema
                        flights.append(&mut batches);

                        let output = futures::stream::iter(flights);

                        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
                    }
                    other => Err(Status::invalid_argument(format!("Invalid Ballista action: {:?}", other))),
                }
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {:?}", e))),
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        println!("get_schema()");

        // let request = request.into_inner();
        //
        // let table = ParquetTable::try_new(&request.path[0]).unwrap();
        //
        // Ok(Response::new(SchemaResult::from(table.schema().as_ref())))

        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info");

        // let request = request.into_inner();
        //
        // let table = ParquetTable::try_new(&request.path[0]).unwrap();
        //
        // let schema_bytes = schema_to_bytes(table.schema().as_ref());
        //
        // Ok(Response::new(FlightInfo {
        //     schema: schema_bytes,
        //     endpoint: vec![],
        //     flight_descriptor: None,
        //     total_bytes: -1,
        //     total_records: -1,
        //
        // }))

        Err(Status::unimplemented("Not yet implemented"))
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
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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

fn to_tonic_err(e: &datafusion::error::ExecutionError) -> Status {
    Status::internal(format!("{:?}", e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    println!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
