// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::Schema;
use arrow::flight::flight_data_to_batch;
//use flight::flight_descriptor;
use flight::flight_service_client::FlightServiceClient;
use flight::FlightDescriptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let testdata = std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    let request = tonic::Request::new(FlightDescriptor {
        r#type: 1, //flight_descriptor::DescriptorType.Path,
        cmd: vec![],
        path: vec![format!("{}/alltypes_plain.parquet", testdata)],
    });

    let flight_info = client.get_flight_info(request).await?.into_inner();
    // let schema = Schema::try_from(&schema_result)?;
    // println!("Schema: {:?}", schema);

    Ok(())
}
