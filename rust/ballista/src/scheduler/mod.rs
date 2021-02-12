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

//! Support for distributed schedulers, such as Kubernetes

pub mod planner;
pub mod state;

use std::convert::TryInto;

use crate::executor::shuffle_reader::ShuffleReaderExec;
use crate::serde::protobuf::{
    job_status, scheduler_grpc_server::SchedulerGrpc, CompletedJob, ExecuteQueryParams,
    ExecuteQueryResult, ExecutorMetadata, FailedJob, GetExecutorMetadataParams,
    GetExecutorMetadataResult, GetJobStatusParams, GetJobStatusResult, JobStatus,
    PartitionLocation, QueuedJob, RegisterExecutorParams, RegisterExecutorResult, RunningJob,
};
use crate::serde::scheduler::ExecutorMeta;
use crate::{client::BallistaClient, error::Result, serde::scheduler::Action};
use crate::{prelude::BallistaError, scheduler::planner::DistributedPlanner};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::context::ExecutionContext;
use log::{debug, error, info, warn};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tonic::{Request, Response};

use self::state::{ConfigBackendClient, SchedulerState};

pub struct SchedulerServer<Config: ConfigBackendClient> {
    state: SchedulerState<Config>,
    namespace: String,
}

impl<Config: ConfigBackendClient> SchedulerServer<Config> {
    pub fn new(config: Config, namespace: String) -> Self {
        Self {
            state: SchedulerState::new(config),
            namespace,
        }
    }
}

#[tonic::async_trait]
impl<T: ConfigBackendClient + Send + Sync + 'static> SchedulerGrpc for SchedulerServer<T> {
    async fn get_executors_metadata(
        &self,
        _request: Request<GetExecutorMetadataParams>,
    ) -> std::result::Result<Response<GetExecutorMetadataResult>, tonic::Status> {
        info!("Received get_executors_metadata request");
        let result = self
            .state
            .get_executors_metadata(self.namespace.as_str())
            .await
            .map_err(|e| {
                let msg = format!("Error reading executors metadata: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?
            .into_iter()
            .map(|meta| meta.into())
            .collect();
        Ok(Response::new(GetExecutorMetadataResult {
            metadata: result,
        }))
    }

    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> std::result::Result<Response<RegisterExecutorResult>, tonic::Status> {
        if let RegisterExecutorParams {
            metadata: Some(metadata),
        } = request.into_inner()
        {
            info!("Received register_executor request for {:?}", metadata);
            self.state
                .save_executor_metadata(&self.namespace, metadata.into())
                .await
                .map_err(|e| {
                    let msg = format!("Could not save executor metadata: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;
            Ok(Response::new(RegisterExecutorResult {}))
        } else {
            warn!("Received invalid executor registration request");
            Err(tonic::Status::invalid_argument(
                "Missing metadata in request",
            ))
        }
    }

    async fn execute_logical_plan(
        &self,
        request: Request<ExecuteQueryParams>,
    ) -> std::result::Result<Response<ExecuteQueryResult>, tonic::Status> {
        if let ExecuteQueryParams {
            logical_plan: Some(logical_plan),
        } = request.into_inner()
        {
            info!("Received execute_logical_plan request");
            let executors = self
                .state
                .get_executors_metadata(&self.namespace)
                .await
                .map_err(|e| {
                    let msg = format!("Error reading executors metadata: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                })?;
            debug!("Found executors: {:?}", executors);

            // parse protobuf
            let plan = (&logical_plan).try_into().map_err(|e| {
                let msg = format!("Could not parse logical plan protobuf: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;

            debug!("Received plan for execution: {:?}", plan);

            let job_id: String = {
                let mut rng = thread_rng();
                std::iter::repeat(())
                    .map(|()| rng.sample(Alphanumeric))
                    .map(char::from)
                    .take(7)
                    .collect()
            };

            // Save placeholder job metadata
            self.state
                .save_job_metadata(
                    &self.namespace,
                    &job_id,
                    &JobStatus {
                        status: Some(job_status::Status::Queued(QueuedJob {})),
                    },
                )
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("Could not save job metadata: {}", e))
                })?;

            // TODO: handle errors once we have more job metadata
            let namespace = self.namespace.to_owned();
            let state = self.state.clone();
            let job_id_spawn = job_id.clone();
            tokio::spawn(async move {
                // create physical plan using DataFusion
                let datafusion_ctx = ExecutionContext::new();
                macro_rules! fail_job {
                    ($code :expr) => {{
                        match $code {
                            Err(error) => {
                                warn!("Job {} failed with {}", job_id_spawn, error);
                                state
                                    .save_job_metadata(
                                        &namespace,
                                        &job_id_spawn,
                                        &JobStatus {
                                            status: Some(job_status::Status::Failed(FailedJob {
                                                error: format!("{}", error),
                                            })),
                                        },
                                    )
                                    .await
                                    .unwrap();
                                return;
                            }
                            Ok(value) => value,
                        }
                    }};
                };
                let plan = fail_job!(datafusion_ctx
                    .optimize(&plan)
                    .and_then(|plan| datafusion_ctx.create_physical_plan(&plan))
                    .map_err(|e| {
                        let msg =
                            format!("Could not build physical execution plan: {}", e);
                        error!("{}", msg);
                        tonic::Status::internal(msg)
                    }));

                // create distributed physical plan using Ballista
                if let Err(e) = state
                    .save_job_metadata(
                        &namespace,
                        &job_id_spawn,
                        &JobStatus {
                            status: Some(job_status::Status::Running(RunningJob {})),
                        },
                    )
                    .await
                {
                    warn!(
                        "Could not update job {} status to running: {}",
                        job_id_spawn, e
                    );
                }
                let mut planner = fail_job!(DistributedPlanner::new(executors).map_err(|e| {
                    let msg = format!("Could not create distributed planner: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                }));
                let plan = fail_job!(planner.execute_distributed_query(plan).await.map_err(|e| {
                    let msg = format!("Could not execute distributed plan: {}", e);
                    error!("{}", msg);
                    tonic::Status::internal(msg)
                }));

                // save partition info into job's state
                let plan = plan
                    .as_any()
                    .downcast_ref::<ShuffleReaderExec>()
                    .expect("Expected plan final operator to be ShuffleReaderExec");
                let mut partition_location = vec![];
                for loc in &plan.partition_location {
                    partition_location.push(loc.clone().try_into().unwrap());
                }
                state
                    .save_job_metadata(
                        &namespace,
                        &job_id_spawn,
                        &JobStatus {
                            status: Some(job_status::Status::Completed(CompletedJob {
                                partition_location,
                            })),
                        },
                    )
                    .await
                    .unwrap();
            });

            Ok(Response::new(ExecuteQueryResult { job_id }))
        } else {
            Err(tonic::Status::internal("Error parsing request"))
        }
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusParams>,
    ) -> std::result::Result<Response<GetJobStatusResult>, tonic::Status> {
        let job_id = request.into_inner().job_id;
        info!("Received get_job_status request for job {}", job_id);
        let job_meta = self
            .state
            .get_job_metadata(&self.namespace, &job_id)
            .await
            .map_err(|e| {
                let msg = format!("Error reading job metadata: {}", e);
                error!("{}", msg);
                tonic::Status::internal(msg)
            })?;
        Ok(Response::new(GetJobStatusResult {
            status: Some(job_meta),
        }))
    }
}
