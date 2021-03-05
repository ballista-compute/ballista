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

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use uuid::Uuid;

use super::protobuf;
use crate::utils::PartitionStats;

pub mod from_proto;
pub mod to_proto;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]

pub enum Action {
    /// Execute a query and store the results in memory
    ExecutePartition(ExecutePartition),
    /// Collect a shuffle partition
    FetchPartition(PartitionId),
}

/// Unique identifier for the output partition of an operator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
}

impl PartitionId {
    pub fn new(job_id: &str, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_id: job_id.to_string(),
            stage_id,
            partition_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub partition_id: PartitionId,
    pub executor_meta: ExecutorMeta,
}

/// Meta-data for an executor, used when fetching shuffle partitions from other executors
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorMeta {
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl Into<protobuf::ExecutorMetadata> for ExecutorMeta {
    fn into(self) -> protobuf::ExecutorMetadata {
        protobuf::ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u32,
        }
    }
}

impl From<protobuf::ExecutorMetadata> for ExecutorMeta {
    fn from(meta: protobuf::ExecutorMetadata) -> Self {
        Self {
            id: meta.id,
            host: meta.host,
            port: meta.port as u16,
        }
    }
}

/// Task that can be sent to an executor to execute one stage of a query and write
/// results out to disk
#[derive(Debug, Clone)]
pub struct ExecutePartition {
    /// Unique ID representing this query execution
    pub job_id: String,
    /// Unique ID representing this query stage within the overall query
    pub stage_id: usize,
    /// The partitions to execute. The same plan could be sent to multiple executors and each
    /// executor will execute a range of partitions per QueryStageTask
    pub partition_id: Vec<usize>,
    /// The physical plan for this query stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Location of shuffle partitions that this query stage may depend on
    pub shuffle_locations: HashMap<PartitionId, ExecutorMeta>,
}

impl ExecutePartition {
    pub fn new(
        job_id: String,
        stage_id: usize,
        partition_id: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_locations: HashMap<PartitionId, ExecutorMeta>,
    ) -> Self {
        Self {
            job_id,
            stage_id,
            partition_id,
            plan,
            shuffle_locations,
        }
    }

    pub fn key(&self) -> String {
        format!("{}.{}.{:?}", self.job_id, self.stage_id, self.partition_id)
    }
}

#[derive(Debug)]
pub struct ExecutePartitionResult {
    /// Path containing results for this partition
    path: String,
    stats: PartitionStats,
}

impl ExecutePartitionResult {
    pub fn new(path: &str, stats: PartitionStats) -> Self {
        Self {
            path: path.to_owned(),
            stats,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}
