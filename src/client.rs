//! Ballista client for sending a query plan to a Ballista executor for execution

#![deny(warnings, rust_2018_idioms)]

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};
use tower_grpc::Request;
use tower_hyper::{client, util};
use tower_util::MakeService;

use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::proto;
use crate::proto::client::Executor;

pub struct Client {
    host: String,
    port: usize,
}

impl Client {
    /// Create a new client to send queries to the specified host/port.
    pub fn new(host: &str, port: usize) -> Self {
        Self {
            host: host.to_string(),
            port,
        }
    }

    /// Blocking call to send a query and wait for the result set
    pub fn send(
        &self,
        plan: LogicalPlan,
        table_meta: Vec<proto::TableMeta>,
    ) -> Result<proto::ExecuteResponse> {
        // send the query to the server
        let uri: http::Uri = format!("http://{}:{}", self.host, self.port)
            .parse()
            .unwrap();
        let dst = Destination::try_from_uri(uri.clone()).unwrap();
        let connector = util::Connector::new(HttpConnector::new(4));
        let settings = client::Builder::new().http2_only(true).clone();
        let mut make_client = client::Connect::with_builder(connector, settings);

        // TODO use channels
        let mut results: Arc<Mutex<Vec<proto::ExecuteResponse>>> = Arc::new(Mutex::new(vec![]));

        let mut final_result = results.clone();

        let execute = make_client
            .make_service(dst)
            .map_err(|e| panic!("connect error: {:?}", e))
            .and_then(move |conn| {
                let conn = tower_request_modifier::Builder::new()
                    .set_origin(uri)
                    .build(conn)
                    .unwrap();

                // Wait until the client is ready...
                Executor::new(conn).ready()
            })
            .and_then(move |mut client| {
                client.execute(Request::new(proto::ExecuteRequest {
                    plan: Some(plan.to_proto()),
                    table_meta,
                }))
            })
            .and_then(move |response| {
                let mut lock = results.borrow_mut().lock().unwrap();
                lock.push(response.get_ref().clone());
                Ok(())
            })
            .map_err(|e| {
                println!("ERR = {:?}", e);
            });

        tokio::run(execute);

        let lock = final_result.borrow_mut().lock().unwrap();
        Ok(lock[0].clone())
    }
}
