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

use crate::scheduler::PhysicalPlan::HashAggregate;
use uuid::Uuid;

struct DistributedPlan {
    stages: Vec<Stage>,
}

struct Stage {
    /// Physical plan with same number of input and output partitions
    partitions: Vec<PhysicalPlan>,
}

#[derive(Debug, Clone)]
enum PhysicalPlan {
    ParquetScan {
        projection: Vec<usize>,
        /// Each partition can process multiple files
        files: Vec<Vec<String>>,
    },
    Projection {
        expr: Vec<Expr>,
        partitions: Vec<PhysicalPlan>,
    },
    Selection {
        partitions: Vec<PhysicalPlan>,
    },
    HashAggregate {
        partitions: Vec<PhysicalPlan>,
    },
    Exchange {
        task_ids: Vec<String>,
    },
}

impl PhysicalPlan {
    fn partition_count(&self) -> usize {
        match self {
            PhysicalPlan::ParquetScan { files, .. } => files.len(),
            PhysicalPlan::Projection { partitions, .. } => partitions.len(),
            PhysicalPlan::Selection { partitions } => partitions.len(),
            PhysicalPlan::HashAggregate { partitions } => partitions.len(),
            _ => unimplemented!(),
        }
    }
}

fn foo(logical_plan: &LogicalPlan) -> Result<DistributedPlan> {
    let mut distributed_plan = DistributedPlan { stages: vec![] };

    let plan = create_scheduler_plan(&mut distributed_plan, logical_plan)?;

    distributed_plan.stages.push(Stage { partitions: plan });

    Ok(distributed_plan)
}

fn create_scheduler_plan(
    distributed_plan: &mut DistributedPlan,
    logical_plan: &LogicalPlan,
) -> Result<Vec<PhysicalPlan>> {
    match logical_plan {
        LogicalPlan::ParquetScan { .. } => {
            // how many partitions? what is the partitioning?

            let mut partitions = vec![];

            //TODO add all the files/partitions
            partitions.push(PhysicalPlan::ParquetScan {
                projection: vec![],
                files: vec![],
            });

            Ok(partitions)
        }

        LogicalPlan::Projection { input, .. } => {
            // no change in partitioning

            let input = create_scheduler_plan(distributed_plan, input)?;

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
            let _input = create_scheduler_plan(distributed_plan, input)?;

            unimplemented!()
        }

        LogicalPlan::Aggregate { input, .. } => {
            let input = create_scheduler_plan(distributed_plan, input)?;

            let stage = Stage {
                partitions: input
                    .iter()
                    .map(|p| HashAggregate {
                        partitions: vec![p.clone()],
                    })
                    .collect(),
            };
            distributed_plan.stages.push(stage);

            Ok(vec![PhysicalPlan::HashAggregate {
                partitions: vec![PhysicalPlan::Exchange { task_ids: vec![] }],
            }])
        }

        _ => unimplemented!(),
    }
}
