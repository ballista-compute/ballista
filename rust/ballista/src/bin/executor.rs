#![feature(async_closure)]

use std::pin::Pin;
use std::thread;
use std::time;

use ballista::datafusion::execution::context::ExecutionContext;
use ballista::serde::decode_protobuf;

use ballista::{plan, BALLISTA_VERSION};

use flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

use etcd_client::*;
use futures::Stream;
use structopt::StructOpt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

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

                        // create the query plan
                        let optimized_plan =
                            ctx.optimize(&logical_plan).map_err(|e| to_tonic_err(&e))?;

                        println!("Optimized Plan: {:?}", optimized_plan);

                        let batch_size = 1024 * 1024;
                        let physical_plan = ctx
                            .create_physical_plan(&optimized_plan, batch_size)
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
                        println!("physical plan schema: {:?}", &schema);

                        let mut flights: Vec<Result<FlightData, Status>> =
                            vec![Ok(FlightData::from(schema.as_ref()))];

                        let mut batches: Vec<Result<FlightData, Status>> = results
                            .iter()
                            .map(|batch| {
                                println!("batch schema: {:?}", batch.schema());

                                Ok(FlightData::from(batch))
                            })
                            .collect();

                        // append batch vector to schema vector, so that the first message sent is the schema
                        flights.append(&mut batches);

                        let output = futures::stream::iter(flights);

                        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
                    }
                    other => Err(Status::invalid_argument(format!(
                        "Invalid Ballista action: {:?}",
                        other
                    ))),
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

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    // List of urls for etcd servers
    #[structopt(long)]
    etcd: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let bind_host_port = "0.0.0.0:50051";

    //TODO need to publish a public host and port - either pass this in on command line or
    // get it from k8s env vars?
    let public_host_port = bind_host_port.clone();

    let addr = bind_host_port.parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    println!("Ballista v{} Rust Executor", BALLISTA_VERSION);

    match opt.etcd {
        Some(urls) => {
            println!("Running in cluster mode");
            println!("Connecting to etcd at {}", urls);

            let mut client = Client::connect([&urls], None).await?;

            let uuid = Uuid::new_v4();

            //TODO make configurable
            let cluster_name = "default";
            let lease_time_seconds = 60;

            let key = format!("/ballista/{}/{}", cluster_name, uuid);
            let value = public_host_port;

            let lease = client.lease_grant(lease_time_seconds, None).await?;
            println!("lease_grant: {:?}", lease);

            let options = PutOptions::new().with_lease(lease.id());
            let resp = client.put(key.clone(), value, Some(options)).await?;
            println!("Registered with etcd as {}. Response: {:?}.", key, resp);

            // keep lease alive in background thread
            //TODO not sure how to do this
            // let lease_id = lease.id();
            // let _ = tokio::spawn(async move || {
            //     loop {
            //         client.lease_keep_alive(lease_id).await?;
            //         thread::sleep(time::Duration::from_secs(5));
            //     }
            // });

        }
        None => {
            println!("Running in standalone mode");
        }
    };

    println!("Flight service listening on {:?}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
