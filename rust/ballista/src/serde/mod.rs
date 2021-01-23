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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use std::convert::TryInto;
use std::io::Cursor;

use crate::error::BallistaError;
use crate::serde::scheduler::Action as BallistaAction;

use prost::Message;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod logical_plan;
pub mod physical_plan;
pub mod scheduler;

pub(crate) fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);
    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> BallistaError {
    BallistaError::General(message.into())
}

/// Create an empty PhysicalPlanNode
pub fn empty_physical_plan_node() -> protobuf::PhysicalPlanNode {
    protobuf::PhysicalPlanNode {
        scan: None,
        input: None,
        projection: None,
        selection: None,
        global_limit: None,
        local_limit: None,
        shuffle_reader: None,
        hash_aggregate: None,
    }
}
