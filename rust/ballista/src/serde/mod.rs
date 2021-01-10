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

// use std::convert::TryInto;
// use std::io::Cursor;
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

pub const BALLISTA_PROTO_VERSION: &str = env!("CARGO_PKG_VERSION");

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod logical_plan;
pub mod physical_plan;

// pub(crate) fn decode_protobuf(bytes: &[u8]) -> Result<Action, BallistaError> {
//     let mut buf = Cursor::new(bytes);
//     protobuf::Action::decode(&mut buf)
//         .map_err(|e| BallistaProtoError::General(format!("{:?}", e)))
//         .and_then(|node| (&node).try_into())
// }

pub(crate) fn proto_error(message: &str) -> BallistaProtoError {
    BallistaProtoError::General(message.to_owned())
}

/// Error
#[derive(Debug)]
pub enum BallistaProtoError {
    General(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
}

impl From<ArrowError> for BallistaProtoError {
    fn from(e: ArrowError) -> Self {
        BallistaProtoError::ArrowError(e)
    }
}

impl From<DataFusionError> for BallistaProtoError {
    fn from(e: DataFusionError) -> Self {
        BallistaProtoError::DataFusionError(e)
    }
}

/// Create an empty ExprNode
pub fn empty_expr_node() -> protobuf::LogicalExprNode {
    protobuf::LogicalExprNode {
        alias: None,
        column_name: "".to_owned(),
        has_column_name: false,
        literal_string: "".to_owned(),
        has_literal_string: false,
        literal_int: 0,
        literal_uint: 0,
        literal_f32: 0.0,
        literal_f64: 0.0,
        has_literal_i8: false,
        has_literal_i16: false,
        has_literal_i32: false,
        has_literal_i64: false,
        has_literal_u8: false,
        has_literal_u16: false,
        has_literal_u32: false,
        has_literal_u64: false,
        has_literal_f32: false,
        has_literal_f64: false,
        binary_expr: None,
        aggregate_expr: None,
    }
}

/// Create an empty LogicalPlanNode
pub fn empty_logical_plan_node() -> protobuf::LogicalPlanNode {
    protobuf::LogicalPlanNode {
        csv_scan: None,
        parquet_scan: None,
        input: None,
        projection: None,
        selection: None,
        limit: None,
        aggregate: None,
        join: None,
    }
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

#[cfg(test)]
mod tests {
    use crate::serde::*;

    #[test]
    fn sanity_check() {
        let _ = empty_logical_plan_node();
        let _ = empty_expr_node();
        let _ = empty_physical_plan_node();
    }
}
