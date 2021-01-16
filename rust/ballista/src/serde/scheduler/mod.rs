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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use uuid::Uuid;

pub mod from_proto;
pub mod to_proto;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Execute the query and return the results
    InteractiveQuery {
        plan: LogicalPlan,
        settings: HashMap<String, String>,
    },
}

/// Unique identifier for the output shuffle partition of an operator.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShuffleId {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
}

impl ShuffleId {
    pub fn new(job_uuid: Uuid, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorMeta {
    pub id: String,
    pub host: String,
    pub port: usize,
}

/// Task that can be sent to an executor for execution
#[derive(Debug, Clone)]
pub struct ExecutionTask {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
}

impl ExecutionTask {
    pub fn new(
        job_uuid: Uuid,
        stage_id: usize,
        partition_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_locations: HashMap<ShuffleId, ExecutorMeta>,
    ) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
            plan,
            shuffle_locations,
        }
    }

    pub fn key(&self) -> String {
        format!("{}.{}.{}", self.job_uuid, self.stage_id, self.partition_id)
    }
}
