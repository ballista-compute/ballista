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

use ballista::prelude::*;
use datafusion::logical_plan::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let plan = LogicalPlanBuilder::scan_parquet(
        "/mnt/tpch/parquet-sf100-partitioned/customer/",
        Some(vec![3, 4]),
        10,
    )?
    .limit(10)?
    .build()?;

    let mut client = BallistaClient::try_new("localhost", 8000).await?;
    let batches = client.execute_query(&plan).await?;
    batches.iter().for_each(|b| println!("{:?}", b));

    Ok(())
}
