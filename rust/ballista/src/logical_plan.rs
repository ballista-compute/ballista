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

//! Ballista Logical Plan

use crate::datafusion::logicalplan::LogicalPlan;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Execute the query and return the results
    Collect { plan: LogicalPlan },
    /// Execute the query and write the results to CSV
    WriteCsv { plan: LogicalPlan, path: String },
    /// Execute the query and write the results to Parquet
    WriteParquet { plan: LogicalPlan, path: String },
}
