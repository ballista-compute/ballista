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

//! Distributed query execution
//!
//! This code is EXPERIMENTAL and still under development

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::client::BallistaClient;
use crate::context::DFTableAdapter;
use crate::error::Result;
use crate::executor::query_stage::QueryStageExec;
use crate::serde::scheduler::ExecutorMeta;
use crate::serde::scheduler::PartitionId;

use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub(crate) partition_id: PartitionId,
    pub(crate) executor_meta: ExecutorMeta,
}

/// Trait that the distributed planner uses to get a list of available executors
pub trait SchedulerClient {
    fn get_executors(&self) -> Result<Vec<ExecutorMeta>>;
}

impl SchedulerClient for Vec<ExecutorMeta> {
    fn get_executors(&self) -> Result<Vec<ExecutorMeta>> {
        Ok(self.clone())
    }
}

pub struct DistributedPlanner {
    scheduler_client: Box<dyn SchedulerClient>,
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn new(scheduler_client: Box<dyn SchedulerClient>) -> Self {
        Self {
            scheduler_client,
            next_stage_id: 0,
        }
    }
}

impl DistributedPlanner {
    pub async fn execute_distributed_query(
        &mut self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let job_uuid = Uuid::new_v4();
        pretty_print(execution_plan.clone(), 0);

        let execution_plan = self.prepare_query_stages(&job_uuid, execution_plan)?;
        pretty_print(execution_plan.clone(), 0);

        let executors = self.scheduler_client.get_executors()?;

        execute(execution_plan.clone(), executors.clone())
            .await
            .await?;

        Ok(())
    }

    /// Insert QueryStageExec nodes into the plan wherever partitioning changes
    pub fn prepare_query_stages(
        &mut self,
        job_uuid: &Uuid,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok(execution_plan.clone());
        }

        let children: Vec<Arc<dyn ExecutionPlan>> = execution_plan
            .children()
            .iter()
            .map(|c| self.prepare_query_stages(&job_uuid, c.clone()))
            .collect::<Result<Vec<_>>>()?;

        if let Some(adapter) = execution_plan.as_any().downcast_ref::<DFTableAdapter>() {
            let ctx = ExecutionContext::new();
            Ok(ctx.create_physical_plan(&adapter.logical_plan)?)
        } else if let Some(merge) = execution_plan.as_any().downcast_ref::<MergeExec>() {
            let child = merge.children()[0].clone();
            Ok(Arc::new(QueryStageExec::try_new(
                *job_uuid,
                self.next_stage_id(),
                child,
            )?))
        } else if let Some(agg) = execution_plan.as_any().downcast_ref::<HashAggregateExec>() {
            //TODO should insert query stages in more generic way based on partitioning metadata
            // and not specifically for this operator
            match agg.mode() {
                AggregateMode::Final => {
                    let children = children
                        .iter()
                        .map(|plan| {
                            create_query_stage(job_uuid, self.next_stage_id(), plan.clone())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(agg.with_new_children(children)?)
                }
                AggregateMode::Partial => Ok(agg.with_new_children(children)?),
            }
        } else if let Some(join) = execution_plan.as_any().downcast_ref::<HashJoinExec>() {
            Ok(join.with_new_children(vec![
                create_query_stage(&*job_uuid, self.next_stage_id(), join.left().clone())?,
                create_query_stage(&*job_uuid, self.next_stage_id(), join.right().clone())?,
            ])?)
        } else {
            // TODO check for compatible partitioning schema, not just count
            if execution_plan.output_partitioning().partition_count()
                != children[0].output_partitioning().partition_count()
            {
                let children = children
                    .iter()
                    .map(|plan| create_query_stage(job_uuid, self.next_stage_id(), plan.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(execution_plan.with_new_children(children)?)
            } else {
                Ok(execution_plan.with_new_children(children)?)
            }
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

/// Visitor pattern to walk the plan, depth-first, and then execute query stages when walking
/// up the tree
async fn execute(
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Pin<Box<dyn Future<Output = Result<Vec<PartitionLocation>>>>> {
    let executors = executors.to_vec();
    Box::pin(async move {
        let mut partition_locations = vec![];
        for child in plan.children() {
            let xx: Result<Vec<PartitionLocation>> =
                execute(child.clone(), executors.clone()).await.await;
            let mut part_loc = xx.unwrap();
            partition_locations.append(&mut part_loc);
        }
        if let Some(stage) = plan.as_any().downcast_ref::<QueryStageExec>() {
            let mut part_loc = execute_query_stage(
                &stage.job_uuid,
                stage.stage_id,
                stage.children()[0].clone(),
                executors.clone(),
            )
            .await
            .unwrap();
            partition_locations.append(&mut part_loc);
        }

        println!("execute {:?}\n\treturning {:?}", plan, partition_locations);

        Ok(partition_locations)
    })
}

fn create_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(QueryStageExec::try_new(
        *job_uuid,
        stage_id,
        plan.clone(),
    )?))
}

/// Execute a query stage by sending each partition to an executor
async fn execute_query_stage(
    job_uuid: &Uuid,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    executors: Vec<ExecutorMeta>,
) -> Result<Vec<PartitionLocation>> {
    let partition_count = plan.output_partitioning().partition_count();
    let mut meta = Vec::with_capacity(partition_count);

    // TODO make this concurrent by executing all partitions at once instead of one at a time

    for child_partition in 0..partition_count {
        let executor_meta = &executors[executors.len() % child_partition];
        let mut client = BallistaClient::try_new(&executor_meta.host, executor_meta.port as usize)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        let partition_metadata = client
            .execute_partition(*job_uuid, stage_id, child_partition, plan.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("Ballista Error: {:?}", e)))?;

        debug!(
            "Partition {} metadata: {:?}",
            child_partition, partition_metadata
        );

        meta.push(PartitionLocation {
            partition_id: PartitionId::new(*job_uuid, stage_id, child_partition),
            executor_meta: executor_meta.clone(),
        });
    }

    Ok(meta)
}

pub fn pretty_print(plan: Arc<dyn ExecutionPlan>, indent: usize) {
    for _ in 0..indent {
        print!("  ");
    }
    println!("{:?}", plan);
    plan.children()
        .iter()
        .for_each(|c| pretty_print(c.clone(), indent + 1));
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::context::BallistaContext;
//     use arrow::datatypes::{DataType, Field, Schema};
//     use datafusion::execution::context::ExecutionContext;
//     use datafusion::physical_plan::csv::CsvReadOptions;
//     use std::collections::HashMap;
//
//     #[tokio::test]
//     async fn test() -> Result<()> {
//         // assumes benchmark data is available
//         let tpch_data = "../../benchmarks/tpch/data";
//
//         let ctx = BallistaContext::remote("localhost", 50051, HashMap::new());
//
//         let tables = vec!["lineitem", "orders"];
//         for table in &tables {
//             let schema = get_schema(table);
//             let options = CsvReadOptions::new()
//                 .schema(&schema)
//                 .file_extension(".tbl")
//                 .has_header(false)
//                 .delimiter(b'|');
//             ctx.register_csv(table, &format!("{}/{}.tbl", tpch_data, table), options)?;
//         }
//
//         let plan = ctx.sql(
//             "select
//     l_shipmode,
//     sum(case
//             when o_orderpriority = '1-URGENT'
//                 or o_orderpriority = '2-HIGH'
//                 then 1
//             else 0
//         end) as high_line_count,
//     sum(case
//             when o_orderpriority <> '1-URGENT'
//                 and o_orderpriority <> '2-HIGH'
//                 then 1
//             else 0
//         end) as low_line_count
// from
//     lineitem
//         join
//     orders
//     on
//             l_orderkey = o_orderkey
// group by
//     l_shipmode
// order by
//     l_shipmode",
//         )?;
//
//         /*
//               where
//               l_shipmode in ('MAIL', 'SHIP')
//         and l_commitdate < l_receiptdate
//         and l_shipdate < l_commitdate
//         and l_receiptdate >= date '1994-01-01'
//         and l_receiptdate < date '1995-01-01'
//
//                */
//         let df = ExecutionContext::new();
//         let plan = df.optimize(&plan.to_logical_plan())?;
//         let plan = df.create_physical_plan(&plan)?;
//
//         let executors = vec![ExecutorMeta {
//             id: "TBD".to_string(),
//             host: "localhost".to_string(),
//             port: 50051,
//         }];
//
//         let mut planner = DistributedPlanner::new(Box::new(executors));
//         planner.execute_distributed_query(plan).await?;
//
//         Ok(())
//     }
//
//     fn get_schema(table: &str) -> Schema {
//         // note that the schema intentionally uses signed integers so that any generated Parquet
//         // files can also be used to benchmark tools that only support signed integers, such as
//         // Apache Spark
//
//         match table {
//             "part" => Schema::new(vec![
//                 Field::new("p_partkey", DataType::Int32, false),
//                 Field::new("p_name", DataType::Utf8, false),
//                 Field::new("p_mfgr", DataType::Utf8, false),
//                 Field::new("p_brand", DataType::Utf8, false),
//                 Field::new("p_type", DataType::Utf8, false),
//                 Field::new("p_size", DataType::Int32, false),
//                 Field::new("p_container", DataType::Utf8, false),
//                 Field::new("p_retailprice", DataType::Float64, false),
//                 Field::new("p_comment", DataType::Utf8, false),
//             ]),
//
//             "supplier" => Schema::new(vec![
//                 Field::new("s_suppkey", DataType::Int32, false),
//                 Field::new("s_name", DataType::Utf8, false),
//                 Field::new("s_address", DataType::Utf8, false),
//                 Field::new("s_nationkey", DataType::Int32, false),
//                 Field::new("s_phone", DataType::Utf8, false),
//                 Field::new("s_acctbal", DataType::Float64, false),
//                 Field::new("s_comment", DataType::Utf8, false),
//             ]),
//
//             "partsupp" => Schema::new(vec![
//                 Field::new("ps_partkey", DataType::Int32, false),
//                 Field::new("ps_suppkey", DataType::Int32, false),
//                 Field::new("ps_availqty", DataType::Int32, false),
//                 Field::new("ps_supplycost", DataType::Float64, false),
//                 Field::new("ps_comment", DataType::Utf8, false),
//             ]),
//
//             "customer" => Schema::new(vec![
//                 Field::new("c_custkey", DataType::Int32, false),
//                 Field::new("c_name", DataType::Utf8, false),
//                 Field::new("c_address", DataType::Utf8, false),
//                 Field::new("c_nationkey", DataType::Int32, false),
//                 Field::new("c_phone", DataType::Utf8, false),
//                 Field::new("c_acctbal", DataType::Float64, false),
//                 Field::new("c_mktsegment", DataType::Utf8, false),
//                 Field::new("c_comment", DataType::Utf8, false),
//             ]),
//
//             "orders" => Schema::new(vec![
//                 Field::new("o_orderkey", DataType::Int32, false),
//                 Field::new("o_custkey", DataType::Int32, false),
//                 Field::new("o_orderstatus", DataType::Utf8, false),
//                 Field::new("o_totalprice", DataType::Float64, false),
//                 Field::new("o_orderdate", DataType::Date32, false),
//                 Field::new("o_orderpriority", DataType::Utf8, false),
//                 Field::new("o_clerk", DataType::Utf8, false),
//                 Field::new("o_shippriority", DataType::Int32, false),
//                 Field::new("o_comment", DataType::Utf8, false),
//             ]),
//
//             "lineitem" => Schema::new(vec![
//                 Field::new("l_orderkey", DataType::Int32, false),
//                 Field::new("l_partkey", DataType::Int32, false),
//                 Field::new("l_suppkey", DataType::Int32, false),
//                 Field::new("l_linenumber", DataType::Int32, false),
//                 Field::new("l_quantity", DataType::Float64, false),
//                 Field::new("l_extendedprice", DataType::Float64, false),
//                 Field::new("l_discount", DataType::Float64, false),
//                 Field::new("l_tax", DataType::Float64, false),
//                 Field::new("l_returnflag", DataType::Utf8, false),
//                 Field::new("l_linestatus", DataType::Utf8, false),
//                 Field::new("l_shipdate", DataType::Date32, false),
//                 Field::new("l_commitdate", DataType::Date32, false),
//                 Field::new("l_receiptdate", DataType::Date32, false),
//                 Field::new("l_shipinstruct", DataType::Utf8, false),
//                 Field::new("l_shipmode", DataType::Utf8, false),
//                 Field::new("l_comment", DataType::Utf8, false),
//             ]),
//
//             "nation" => Schema::new(vec![
//                 Field::new("n_nationkey", DataType::Int32, false),
//                 Field::new("n_name", DataType::Utf8, false),
//                 Field::new("n_regionkey", DataType::Int32, false),
//                 Field::new("n_comment", DataType::Utf8, false),
//             ]),
//
//             "region" => Schema::new(vec![
//                 Field::new("r_regionkey", DataType::Int32, false),
//                 Field::new("r_name", DataType::Utf8, false),
//                 Field::new("r_comment", DataType::Utf8, false),
//             ]),
//
//             _ => unimplemented!(),
//         }
//     }
// }
