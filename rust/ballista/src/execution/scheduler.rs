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
use crate::execution::shuffle_exchange::ShuffleExchangeExec;
use std::rc::Rc;

pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Rc<PhysicalPlan>> {
    match plan {
        LogicalPlan::Projection { input, .. } => {
            let exec = ProjectionExec::new(create_physical_plan(input)?);
            Ok(Rc::new(PhysicalPlan::Projection(exec)))
        }
        LogicalPlan::Aggregate { input, .. } => {
            let input = create_physical_plan(input)?;
            if input
                .as_execution_plan()
                .output_partitioning()
                .partition_count()
                == 1
            {
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(AggregateMode::Final, input),
                ))))
            } else {
                let partial = Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(AggregateMode::Partial, input),
                )));
                Ok(Rc::new(PhysicalPlan::HashAggregate(Rc::new(
                    HashAggregateExec::new(AggregateMode::Final, partial),
                ))))
            }
        }
        _ => Err(BallistaError::General("unsupported".to_string())),
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
