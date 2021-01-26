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

use crate::{
    error::{ballista_error, BallistaError},
    serde::{protobuf, scheduler::Action},
};
use std::convert::TryInto;

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::InteractiveQuery { ref plan, ref settings } => {
                let plan_proto: protobuf::LogicalPlanNode = plan.try_into()?;

                let settings = settings
                    .iter()
                    .map(|e| protobuf::KeyValuePair {
                        key: e.0.to_string(),
                        value: e.1.to_string(),
                    })
                    .collect();

                Ok(protobuf::Action {
                    query: Some(plan_proto),
                    task: None,
                    fetch_shuffle: None,
                    settings,
                })
            }
            // Action::ExecuteTask(task) => Ok(protobuf::Action {
            //     query: None,
            //     task: Some(task.try_into()?),
            //     fetch_shuffle: None,
            //     settings: vec![],
            // }),
            // Action::FetchShuffle(shuffle_id) => Ok(protobuf::Action {
            //     query: None,
            //     task: None,
            //     fetch_shuffle: Some(shuffle_id.try_into()?),
            //     settings: vec![],
            // }),
            _ => Err(ballista_error("scheduler::to_proto() unimplemented Action")),
        }
    }
}
