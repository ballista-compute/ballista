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

use crate::{error::BallistaError,
            serde::{proto_error, protobuf, scheduler::Action}};

use datafusion::logical_plan::LogicalPlan;

macro_rules! convert_required {
    ($PB:expr) => {{

        if let Some(field) = $PB.as_ref() {

            field.try_into()
        } else {

            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

// macro_rules! convert_box_required {
//     ($PB:expr) => {{
//         if let Some(field) = $PB.as_ref() {
//             field.as_ref().try_into()
//         } else {
//             Err(proto_error("Missing required field in protobuf"))
//         }
//     }};
// }

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {

        if self.query.is_some() {

            let plan: LogicalPlan = convert_required!(self.query)?;

            let mut settings = HashMap::new();

            for setting in &self.settings {

                settings.insert(setting.key.to_owned(), setting.value.to_owned());
            }

            Ok(Action::InteractiveQuery { plan, settings })
        // } else if self.task.is_some() {
        //     let task: ExecutionTask = convert_required!(self.task)?;
        //     Ok(Action::Execute(task))
        // } else if self.fetch_shuffle.is_some() {
        //     let shuffle_id: ShuffleId = convert_required!(self.fetch_shuffle)?;
        //     Ok(Action::FetchShuffle(shuffle_id))
        } else {

            Err(BallistaError::NotImplemented(format!("from_proto(Action) {:?}", self)))
        }
    }
}
