// Copyright 2021 Andy Grove
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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::sync::Arc;
use std::{convert::TryInto, unimplemented};

use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort::{SortExec, SortOptions};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::convert_box_required;
use crate::error::BallistaError;
use crate::serde::{proto_error, protobuf};

use protobuf::physical_plan_node::PhysicalPlanType;

impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(projection.input)?;
                let exprs = projection
                    .expr
                    .iter()
                    .map(|expr| expr.try_into().map(|e| (e, "unused".to_string())))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or(
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self)))?;
                        if let protobuf::logical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr.expr.as_ref().ok_or(
                                proto_error(format!(
                                    "physical_plan::from_proto() Unexpected sort expr {:?}",
                                    self)))?.as_ref();
                            Ok(PhysicalSortExpr {
                                expr: expr.try_into()?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                }
                            })
                        } else {
                            Err(BallistaError::General(format!(
                                "physical_plan::from_proto() {:?}", self)))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(SortExec::try_new(exprs, input, sort.concurrency as usize)?))
            }
            _ => unimplemented!(),
        }
    }
}

impl TryInto<Arc<dyn PhysicalExpr>> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn PhysicalExpr>, Self::Error> {
        unimplemented!()
    }
}
