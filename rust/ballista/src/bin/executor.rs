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

use ballista::distributed::flight_service::FlightServiceImpl;
use ballista::BALLISTA_VERSION;

use flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightServiceImpl::new();
    let svc = FlightServiceServer::new(service);

    println!(
        "Ballista v{} Rust Executor listening on {:?}",
        BALLISTA_VERSION, addr
    );

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
