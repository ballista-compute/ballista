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

//! The Ballista distributed scheduler translates a logical plan into a physical plan consisting
//! of tasks that can be scheduled across a number of executors (which could be multiple threads
//! in a single process, or multiple processes in a cluster).

use crate::datafusion::logicalplan::Expr;
use crate::datafusion::logicalplan::LogicalPlan;

use crate::error::Result;

use uuid::Uuid;

/// A physical plan that can be assigned to an executor
struct Task {
    /// Task ID
    id: String,
    /// The partition of the plan to execute
    partition_index: usize,
    /// Physical plan with same number of input and output partitions
    plan: PhysicalPlan,
}

struct PartitionKey {}

struct SortOrder {}

#[derive(Debug, Clone)]
enum PhysicalPlan {
    ParquetPartitionScan {
        projection: Vec<usize>,
        /// Each partition can process multiple files
        files: Vec<String>,
    },
    Projection {
        expr: Vec<Expr>,
        partitions: Vec<PhysicalPlan>,
    },
    Selection {
        partitions: Vec<PhysicalPlan>,
    },
    PartialHashAggregate {
        partitions: Vec<PhysicalPlan>,
    },
    FinalHashAggregate {
        partitions: Vec<PhysicalPlan>,
    },
    Task {
        id: String,
        plan: Box<PhysicalPlan>
    },
    Exchange {
        task_ids: Vec<String>
    },
}

impl PhysicalPlan {
    fn partition_count(&self) -> usize {
        match self {
            PhysicalPlan::ParquetPartitionScan { .. } => 1,
            PhysicalPlan::Projection { partitions, .. } => partitions.len(),
            PhysicalPlan::Selection { partitions } => partitions.len(),
            PhysicalPlan::PartialHashAggregate { partitions } => partitions.len(),
            PhysicalPlan::FinalHashAggregate { .. } => 1,
            _ => unimplemented!(),
        }
    }
}

fn create_scheduler_plan(plan: &LogicalPlan) -> Result<Vec<PhysicalPlan>> {
    match plan {
        LogicalPlan::ParquetScan { path, .. } => {
            // how many partitions? what is the partitioning?

            let mut partitions = vec![];

            //TODO add all the partitions
            partitions.push(PhysicalPlan::ParquetScan {
                projection: vec![],
                partitions: vec![],
            });

            Ok(partitions)
        }

        LogicalPlan::Projection { input, .. } => {
            // no change in partitioning

            let input = create_scheduler_plan(input)?;

            Ok(input
                .iter()
                .map(|plan| PhysicalPlan::Projection {
                    expr: vec![],
                    partitions: vec![plan.clone()],
                })
                .collect())
        }

        LogicalPlan::Selection { input, .. } => {
            // no change in partitioning
            let input = create_scheduler_plan(input)?;

            unimplemented!()
        }

        LogicalPlan::Aggregate { input, .. } => {
            let input = create_scheduler_plan(input)?;

            //TODO Create multiple of these
            PhysicalPlan::PartialHashAggregate { partitions: vec![] };

            PhysicalPlan::Exchange {

            };

            PhysicalPlan::FinalHashAggregate { partitions: vec![] };

            //let task_id = Uuid::new_v4();

            // produces a single partition
            unimplemented!()
        }

        _ => unimplemented!(),
    }
}
