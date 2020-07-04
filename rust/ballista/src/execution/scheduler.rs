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

use crate::datafusion::logicalplan::LogicalPlan;
use crate::error::{BallistaError, Result};
use crate::execution::physical_plan::{AggregateMode, Distribution, Partitioning, PhysicalPlan};
use crate::execution::projection::ProjectionExec;

use crate::execution::hash_aggregate::HashAggregateExec;
use crate::execution::parquet_scan::ParquetScanExec;
use crate::execution::shuffle_exchange::ShuffleExchangeExec;
use std::rc::Rc;

pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Rc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, .. } => {
            let exec = ProjectionExec::new(create_physical_plan(input)?);
            Ok(Rc::new(PhysicalPlan::Projection(exec)))
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } => {
            let input = create_physical_plan(input)?;
            if input
                .as_execution_plan()
                .output_partitioning()
                .partition_count()
                == 1
            {
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Final,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        input,
                    ),
                ))))
            } else {
                let partial = Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Partial,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        input,
                    ),
                )));
                // TODO these are not the correct expressions being passed in here for the final agg
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(
                        AggregateMode::Final,
                        group_expr.clone(),
                        aggr_expr.clone(),
                        partial,
                    ),
                ))))
            }
        }
        LogicalPlan::ParquetScan { path, .. } => {
            let exec = ParquetScanExec::try_new(&path, None)?;
            Ok(Rc::new(PhysicalPlan::ParquetScan(exec)))
        }
        other => Err(BallistaError::General(format!("unsupported {:?}", other))),
    }
}

/// Optimizer rule to insert shuffles as needed
pub fn ensure_requirements(plan: &PhysicalPlan) -> Result<Rc<PhysicalPlan>> {
    let execution_plan = plan.as_execution_plan();

    // recurse down and replace children
    if execution_plan.children().is_empty() {
        return Ok(Rc::new(plan.clone()));
    }
    let children: Vec<Rc<PhysicalPlan>> = execution_plan
        .children()
        .iter()
        .map(|c| ensure_requirements(c.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    match execution_plan.required_child_distribution() {
        Distribution::SinglePartition => {
            let new_children: Vec<Rc<PhysicalPlan>> = children
                .iter()
                .map(|c| {
                    if c.as_execution_plan()
                        .output_partitioning()
                        .partition_count()
                        > 1
                    {
                        Rc::new(PhysicalPlan::ShuffleExchange(Rc::new(
                            ShuffleExchangeExec::new(
                                c.clone(),
                                Partitioning::UnknownPartitioning(1),
                            ),
                        )))
                    } else {
                        Rc::new(plan.clone())
                    }
                })
                .collect();

            Ok(Rc::new(plan.with_new_children(new_children)))
        }
        _ => Ok(Rc::new(plan.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{col, sum, Context};
    use std::collections::HashMap;
    //use crate::arrow::datatypes::{Schema, Field, DataType};

    // #[test]
    // fn create_plan() -> Result<()> {
    //     let ctx = Context::local(HashMap::new());
    //
    //     let df = ctx
    //         .read_parquet("/mnt/nyctaxi/parquet/year=2019", None)?
    //         .aggregate(vec![col("passenger_count")], vec![sum(col("fare_amount"))])?;
    //
    //     let plan = df.logical_plan();
    //
    //     let plan = create_physical_plan(&plan)?;
    //     let plan = ensure_requirements(&plan)?;
    //     println!("{:?}", plan);
    //
    //     Ok(())
    // }
}
