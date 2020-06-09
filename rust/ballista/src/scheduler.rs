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
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct DistributedPlan {
    stages: Vec<Stage>,
}

#[derive(Debug, Clone)]
struct Stage {
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
        stage_id: Uuid,
    },
}

// impl PhysicalPlan {
//     fn partition_count(&self) -> usize {
//         match self {
//             PhysicalPlan::ParquetScan { files, .. } => files.len(),
//             PhysicalPlan::Projection { partitions, .. } => partitions.len(),
//             PhysicalPlan::Selection { partitions } => partitions.len(),
//             PhysicalPlan::HashAggregate { partitions } => partitions.len(),
//             _ => unimplemented!(),
//         }
//     }
// }

fn create_distributed_plan(logical_plan: &LogicalPlan) -> Result<DistributedPlan> {
    let mut distributed_plan = DistributedPlan { stages: vec![] };

    let partitions = create_scheduler_plan(&mut distributed_plan, logical_plan)?;

    distributed_plan.stages.push(Stage::new(partitions));

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

            let stage = Stage::new(
                input
                    .iter()
                    .map(|p| HashAggregate {
                        partitions: vec![p.clone()],
                    })
                    .collect(),
            );

            let stage_id = stage.id().to_owned();

            distributed_plan.stages.push(stage);

            Ok(vec![PhysicalPlan::HashAggregate {
                partitions: vec![PhysicalPlan::Exchange { stage_id: stage_id }],
            }])
        }

        _ => unimplemented!(),
    }
}

fn sanitize(s: &Uuid) -> String {
    s.to_string().replace("-", "_")
}

fn create_dot_plan(
    dist_plan: &DistributedPlan,
    stage_id: &str,
    plan: &PhysicalPlan,
    map: &mut HashMap<Uuid, String>,
    stage_output_map: &HashMap<Uuid, String>,
) -> Uuid {
    // uuid for plan step
    let uuid = Uuid::new_v4();
    let dot_id = format!("node{}", map.len());
    map.insert(uuid, dot_id.clone());

    match plan {
        PhysicalPlan::HashAggregate { partitions } => {
            println!("\t\t{} [label=\"HashAggregate\"];", dot_id);
            let child_uuid =
                create_dot_plan(dist_plan, stage_id, &partitions[0], map, stage_output_map);
            let child_dot_id = &map[&child_uuid].as_str();
            println!("\t\t{} -> {};", child_dot_id, dot_id);
        }
        PhysicalPlan::ParquetScan { .. } => {
            println!("\t\t{} [label=\"ParquetScan\"];", dot_id);
        }
        PhysicalPlan::Exchange { stage_id, .. } => {
            println!("\t\t{} [label=\"Exchange\"];", dot_id);

            //stage_output_map
            let other_stage = dist_plan
                .stages
                .iter()
                .find(|stage| stage.id.to_owned() == stage_id.to_owned())
                .unwrap();

            let x = &stage_output_map[other_stage.id()];
            println!("\t\t{} -> {};", x, dot_id);
        }
        _ => {
            println!("other;");
        }
    }
    uuid
}

fn create_dot_file(plan: &DistributedPlan) {
    println!("digraph distributed_plan {{");

    let mut map = HashMap::new();
    let mut stage_output_uuid = HashMap::new();

    let mut cluster = 0;

    for stage in &plan.stages {
        let stage_id = sanitize(stage.id());

        println!("\tsubgraph cluster{} {{", cluster);
        println!("\t\tnode [style=filled];");
        println!("\t\tlabel = \"Stage {}\";", cluster);
        let uuid = create_dot_plan(
            &plan,
            &stage_id,
            &stage.partitions[0],
            &mut map,
            &stage_output_uuid,
        );
        let x: String = map[&uuid].to_owned();
        stage_output_uuid.insert(stage.id().to_owned(), x);

        println!("\t}}");
        cluster += 1;
    }

    println!("}}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{max, DataFrame};
    use datafusion::logicalplan::{col, LogicalPlanBuilder};

    #[test]
    fn create_plan() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_parquet("/mnt/nyctaxi/parquet", None)?
            .aggregate(vec![col("passenger_count")], vec![max(col("fare_amt"))])?
            .build()?;

        let distributed_plan = create_distributed_plan(&plan)?;

        println!("{:?}", distributed_plan);

        create_dot_file(&distributed_plan);

        Ok(())
    }
}
