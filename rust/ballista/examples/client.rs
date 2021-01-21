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

//! Minimal example showing how to execute a query with Ballista
//!
//! This is EXPERIMENTAL and under development still

use ballista::prelude::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let path = "/mnt/tpch/parquet-sf100-partitioned/customer/";

    let ctx = BallistaContext::remote("localhost", 50051, HashMap::new());

    let mut stream = ctx.read_parquet(path)?.limit(10)?.collect().await?;

    while let Some(result) = stream.next().await {
        let batch = result?;
        println!("{:?}", batch)
    }

    Ok(())
}
