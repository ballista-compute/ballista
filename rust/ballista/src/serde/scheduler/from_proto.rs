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

use std::{collections::HashMap, convert::TryInto};

use crate::error::BallistaError;
use crate::scheduler::planner::PartitionLocation;
use crate::serde::protobuf;
use crate::serde::protobuf::action::ActionType;
use crate::serde::scheduler::{Action, ExecutePartition, PartitionId};

use datafusion::logical_plan::LogicalPlan;
use uuid::Uuid;

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self.action_type {
            Some(ActionType::ExecutePartition(partition)) => {
                // TODO remove unwraps
                Ok(Action::ExecutePartition(ExecutePartition::new(
                    partition.job_id,
                    partition.stage_id as usize,
                    partition.partition_id as usize,
                    partition.plan.as_ref().unwrap().try_into()?,
                    HashMap::new(),
                )))
            }
            Some(ActionType::FetchPartition(partition)) => {
                Ok(Action::FetchPartition(partition.try_into()?))
            }
            _ => Err(BallistaError::General(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}

impl TryInto<PartitionId> for protobuf::PartitionId {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionId, Self::Error> {
        Ok(PartitionId::new(
            &self.job_id,
            self.stage_id as usize,
            self.partition_id as usize,
        ))
    }
}

impl TryInto<PartitionLocation> for protobuf::PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionLocation, Self::Error> {
        Ok(PartitionLocation {
            partition_id: self.partition_id.unwrap().try_into()?,
            executor_meta: self.executor_meta.unwrap().into(),
        })
    }
}
