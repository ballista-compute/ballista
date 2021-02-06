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

//! Client API for sending requests to executors.

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::error::{ballista_error, BallistaError, Result};
use crate::memory_stream::MemoryStream;
use crate::serde::protobuf::{self};
use crate::serde::scheduler::{Action, ExecutePartition, ExecutePartitionResult, PartitionId};

use arrow::array::StringArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use log::debug;
use prost::Message;
use uuid::Uuid;

/// Client for interacting with Ballista executors.
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port
    pub async fn try_new(host: &str, port: usize) -> Result<Self> {
        let addr = format!("http://{}:{}", host, port);
        debug!("BallistaClient connecting to {}", addr);
        let flight_client = FlightServiceClient::connect(addr.clone())
            .await
            .map_err(|e| {
                BallistaError::General(format!(
                    "Error connecting to Ballista scheduler or executor at {}: {:?}",
                    addr, e
                ))
            })?;
        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Execute a logical query plan and retrieve the results
    pub async fn execute_query(&mut self, plan: &LogicalPlan) -> Result<SendableRecordBatchStream> {
        let action = Action::InteractiveQuery {
            plan: plan.to_owned(),
            settings: HashMap::new(),
        };
        self.execute_action(&action).await

        //Ok(Arc::new(CollectExec::new(final_stage)))
    }

    /// Execute one partition of a physical query plan against the executor
    pub async fn execute_partition(
        &mut self,
        job_uuid: Uuid,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<ExecutePartitionResult> {
        let action = Action::ExecutePartition(ExecutePartition {
            job_uuid,
            stage_id,
            partition_id,
            plan,
            shuffle_locations: Default::default(),
        });
        let stream = self.execute_action(&action).await?;
        let batches = collect(stream).await?;

        if batches.len() != 1 {
            return Err(BallistaError::General(
                "execute_partition received wrong number of result batches".to_owned(),
            ));
        }

        let batch = &batches[0];
        if batch.num_rows() != 1 {
            return Err(BallistaError::General(
                "execute_partition received wrong number of rows".to_owned(),
            ));
        }

        let path = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("execute_partition expected column 0 to be a StringArray");

        Ok(ExecutePartitionResult::new(path.value(0)))
    }

    /// Fetch a partition from an executor
    pub async fn fetch_partition(
        &mut self,
        job_uuid: &Uuid,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<Vec<RecordBatch>> {
        let action = Action::FetchPartition(PartitionId::new(
            job_uuid.to_owned(),
            stage_id,
            partition_id,
        ));
        let stream = self.execute_action(&action).await?;
        Ok(collect(stream)
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?)
    }

    /// Execute an action and retrieve the results
    pub async fn execute_action(&mut self, action: &Action) -> Result<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;
        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());
        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self
            .flight_client
            .do_get(request)
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        match stream
            .message()
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        {
            Some(flight_data) => {
                // convert FlightData to a stream
                let schema = Arc::new(Schema::try_from(&flight_data)?);

                // all the remaining stream messages should be dictionary and record batches

                //TODO we should stream the data rather than load into memory first
                let mut batches = vec![];
                while let Some(flight_data) = stream
                    .message()
                    .await
                    .map_err(|e| BallistaError::General(format!("{:?}", e)))?
                {
                    let batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])?;
                    batches.push(batch);
                }

                Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
            }
            None => Err(ballista_error(
                "Did not receive schema batch from flight server",
            )),
        }
    }
}
