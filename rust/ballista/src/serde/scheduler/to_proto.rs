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

use std::convert::TryInto;

use crate::error::BallistaError;
use crate::serde::protobuf;
use crate::serde::protobuf::action::ActionType;
use crate::serde::scheduler::{Action, ExecutePartition, PartitionId};

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {

            Action::InteractiveQuery { ref plan, ref settings } => {
                let plan_proto: protobuf::LogicalPlanNode = plan.try_into()?;

            Action::InteractiveQuery {
                ref plan,
                    .map(|e| protobuf::KeyValuePair {
                        key: e.0.to_string(),
                        value: e.1.to_string(),
                    })

                Ok(protobuf::Action {
                    action_type: Some(ActionType::Query(plan.try_into()?)),
                    settings,
                })
            }
            Action::ExecutePartition(partition) => Ok(protobuf::Action {
                action_type: Some(ActionType::ExecutePartition(partition.try_into()?)),
                settings: vec![],
            }),
            Action::FetchPartition(partition_id) => Ok(protobuf::Action {
                action_type: Some(ActionType::FetchPartition(partition_id.try_into()?)),
                settings: vec![],
            }),
        }
    }
}

impl TryInto<protobuf::ExecutePartition> for ExecutePartition {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::ExecutePartition, Self::Error> {
        Ok(protobuf::ExecutePartition {
            job_uuid: self.job_uuid.to_string(),
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
            plan: Some(self.plan.try_into()?),
            partition_location: vec![],
        })
    }
}

impl TryInto<protobuf::PartitionId> for PartitionId {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PartitionId, Self::Error> {
        unimplemented!()
    }
}
