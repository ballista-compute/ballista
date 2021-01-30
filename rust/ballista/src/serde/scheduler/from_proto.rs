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
use std::convert::TryInto;

use crate::error::BallistaError;
use crate::serde::protobuf;
use crate::serde::protobuf::action::ActionType;
use crate::serde::scheduler::Action;

use datafusion::logical_plan::LogicalPlan;

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self.action_type {
            Some(ActionType::Query(query)) => {
                let mut settings = HashMap::new();
                let plan: LogicalPlan = (&query).try_into()?;
                for setting in &self.settings {
                    settings.insert(setting.key.to_owned(), setting.value.to_owned());
                    for setting in &self.settings {
                        settings.insert(setting.key.to_owned(), setting.value.to_owned());
                    }
                }
                Ok(Action::InteractiveQuery { plan, settings })
            }
            _ => Err(BallistaError::General(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}
