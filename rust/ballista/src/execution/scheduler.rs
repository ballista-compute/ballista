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

use crate::execution::scheduler::PhysicalPlan::HashAggregate;
use std::collections::HashMap;
use uuid::Uuid;

/// Executable Job consisting of one or more stages
#[derive(Debug, Clone)]
pub struct Job {
    stages: Vec<Stage>,
}

/// A stage represents a series of parallel tasks with the same partitioning
#[derive(Debug, Clone)]
pub struct Stage {
    /// Stage ID
    id: Uuid,
    /// Physical plan with same number of input and output partitions
    partitions: Vec<PhysicalPlan>,
}

impl Stage {
    fn new(partitions: Vec<PhysicalPlan>) -> Self {
        Self {
            id: Uuid::new_v4(),
            partitions,
        }
    }

    fn id(&self) -> &Uuid {
        &self.id
    }
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
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
        stage_id: Uuid,
    },
}

pub fn create_job(logical_plan: &LogicalPlan) -> Result<Job> {
    let mut job = Job { stages: vec![] };

    let partitions = create_scheduler_plan(&mut job, logical_plan)?;

    job.stages.push(Stage::new(partitions));

    Ok(job)
}

fn create_scheduler_plan(job: &mut Job, logical_plan: &LogicalPlan) -> Result<Vec<PhysicalPlan>> {
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

            let input = create_scheduler_plan(job, input)?;

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
            let _input = create_scheduler_plan(job, input)?;

            unimplemented!()
        }

        LogicalPlan::Aggregate { input, .. } => {
            let input = create_scheduler_plan(job, input)?;

            let stage = Stage::new(
                input
                    .iter()
                    .map(|p| HashAggregate {
                        partitions: vec![p.clone()],
                    })
                    .collect(),
            );

            let stage_id = stage.id().to_owned();

            job.stages.push(stage);

            Ok(vec![PhysicalPlan::HashAggregate {
                partitions: vec![PhysicalPlan::Exchange { stage_id: stage_id }],
            }])
        }

        _ => unimplemented!(),
    }
}

#[allow(dead_code)]
fn create_dot_plan(
    job: &Job,
    plan: &PhysicalPlan,
    physical_plan_to_dot_id_map: &mut HashMap<Uuid, String>,
    stage_to_dot_id_map: &HashMap<Uuid, String>,
) -> Uuid {
    // uuid for plan step
    let uuid = Uuid::new_v4();
    let dot_id = format!("node{}", physical_plan_to_dot_id_map.len());
    physical_plan_to_dot_id_map.insert(uuid, dot_id.clone());

    match plan {
        PhysicalPlan::HashAggregate { partitions } => {
            println!("\t\t{} [label=\"HashAggregate\"];", dot_id);
            let child_uuid = create_dot_plan(
                job,
                &partitions[0],
                physical_plan_to_dot_id_map,
                stage_to_dot_id_map,
            );
            let child_dot_id = &physical_plan_to_dot_id_map[&child_uuid].as_str();
            println!("\t\t{} -> {};", child_dot_id, dot_id);
        }
        PhysicalPlan::ParquetScan { .. } => {
            println!("\t\t{} [label=\"ParquetScan\"];", dot_id);
        }
        PhysicalPlan::Exchange { stage_id, .. } => {
            println!("\t\t{} [label=\"Exchange\"];", dot_id);

            let other_stage = job
                .stages
                .iter()
                .find(|stage| &stage.id == stage_id)
                .unwrap();

            let stage_dot_id = &stage_to_dot_id_map[other_stage.id()];
            println!("\t\t{} -> {};", stage_dot_id, dot_id);
        }
        _ => {
            println!("other;");
        }
    }
    uuid
}

#[allow(dead_code)]
fn create_dot_file(plan: &Job) {
    println!("digraph distributed_plan {{");

    let mut map = HashMap::new();
    let mut stage_output_uuid = HashMap::new();

    for (cluster, stage) in plan.stages.iter().enumerate() {
        println!("\tsubgraph cluster{} {{", cluster);
        println!("\t\tnode [style=filled];");
        println!("\t\tlabel = \"Stage {}\";", cluster);
        let uuid = create_dot_plan(&plan, &stage.partitions[0], &mut map, &stage_output_uuid);
        let stage_dot_id = map[&uuid].to_owned();
        stage_output_uuid.insert(stage.id().to_owned(), stage_dot_id);

        println!("\t}}");
    }

    println!("}}");
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::dataframe::max;
//     use datafusion::logicalplan::{col, LogicalPlanBuilder};
//
//     #[test]
//     fn create_plan() -> Result<()> {
//         let plan = LogicalPlanBuilder::scan_parquet("/mnt/nyctaxi/parquet", None)?
//             .aggregate(vec![col("passenger_count")], vec![max(col("fare_amt"))])?
//             .build()?;
//
//         let job = create_job(&plan)?;
//
//         println!("{:?}", job);
//
//         create_dot_file(&job);
//
//         Ok(())
//     }
// }
