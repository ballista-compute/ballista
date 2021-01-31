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

use datafusion::physical_plan::csv::CsvExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::{
    coalesce_batches::CoalesceBatchesExec,
    empty::EmptyExec,
    expressions::PhysicalSortExpr,
    limit::{GlobalLimitExec, LocalLimitExec},
    projection::ProjectionExec,
    sort::{SortExec, SortOptions},
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::CsvReadOptions;

use crate::error::BallistaError;
use crate::serde::{proto_error, protobuf};
use crate::{convert_box_required, convert_required};

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
            PhysicalPlanType::CsvScan(scan) => {
                let schema = Arc::new(convert_required!(scan.schema)?);
                let options = CsvReadOptions::new()
                    .has_header(scan.has_header)
                    .file_extension(&scan.file_extension)
                    .delimiter(scan.delimiter.as_bytes()[0])
                    .schema(&schema);
                // TODO we don't care what the DataFusion batch size was because Ballista will
                // have its own configs. Hard-code for now.
                let batch_size = 32768;
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                Ok(Arc::new(CsvExec::try_new(
                    &scan.path,
                    options,
                    Some(projection),
                    batch_size,
                )?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                // TODO we don't care what the DataFusion batch size was because Ballista will
                // have its own configs. Hard-code for now.
                let batch_size = 32768;
                let max_concurrency = 8;
                let filenames: Vec<&str> = scan.filename.iter().map(|s| s.as_str()).collect();
                Ok(Arc::new(ParquetExec::try_from_files(
                    &filenames,
                    Some(projection),
                    None,
                    batch_size,
                    max_concurrency,
                )?))
            }
            PhysicalPlanType::Selection(_) => unimplemented!(),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(GlobalLimitExec::new(
                    input,
                    limit.limit as usize,
                    0,
                ))) // TODO: concurrency param doesn't seem to be used in datafusion. not sure how to fill this in
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::HashAggregate(_) => unimplemented!(),
            PhysicalPlanType::ShuffleReader(_) => unimplemented!(),
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::logical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: expr.try_into()?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(BallistaError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // Update concurrency here in the future
                Ok(Arc::new(SortExec::try_new(exprs, input, 1)?))
            }
        }
    }
}

impl TryInto<Arc<dyn PhysicalExpr>> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn PhysicalExpr>, Self::Error> {
        unimplemented!()
    }
}
