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

use std::convert::TryInto;
use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::datafusion::execution::physical_plan::csv::CsvReadOptions;
use crate::datafusion::logicalplan::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarValue,
};

use crate::distributed::scheduler::ExecutionTask;
use crate::error::{ballista_error, BallistaError};
use crate::execution::operators::{HashAggregateExec, ParquetScanExec};
use crate::execution::physical_plan::{Action, ShuffleLocation};
use crate::execution::physical_plan::{AggregateMode, PhysicalPlan};
use crate::protobuf;
use std::collections::HashMap;
use uuid::Uuid;

impl TryInto<LogicalPlan> for protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        if let Some(projection) = self.projection {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            LogicalPlanBuilder::from(&input)
                .project(
                    projection
                        .expr
                        .iter()
                        .map(|expr| expr.to_owned().try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(selection) = self.selection {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            let expr: protobuf::LogicalExprNode = selection.expr.expect("expression required");
            LogicalPlanBuilder::from(&input)
                .filter(expr.try_into()?)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(aggregate) = self.aggregate {
            let input: LogicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            let group_expr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_expr = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            LogicalPlanBuilder::from(&input)
                .aggregate(group_expr, aggr_expr)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(scan) = self.scan {
            let schema: Schema = scan.schema.unwrap().try_into()?;

            let projection: Vec<usize> = scan
                .projection
                .iter()
                .map(|name| schema.index_of(name))
                .collect::<Result<Vec<_>, _>>()?;

            println!("projection: {:?}", projection);

            match scan.file_format.as_str() {
                "csv" => {
                    let options = CsvReadOptions::new()
                        .schema(&schema)
                        .has_header(scan.has_header);
                    LogicalPlanBuilder::scan_csv(
                        &scan.path, options, None, //TODO projection
                    )?
                    .build()
                    .map_err(|e| e.into())
                }
                "parquet" => LogicalPlanBuilder::scan_parquet(&scan.path, None)? //TODO projection
                    .build()
                    .map_err(|e| e.into()),
                other => Err(ballista_error(&format!(
                    "Unsupported file format '{}' for file scan",
                    other
                ))),
            }
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical plan '{:?}'",
                self
            )))
        }
    }
}

impl TryInto<Expr> for protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        if let Some(binary_expr) = self.binary_expr {
            Ok(Expr::BinaryExpr {
                left: Box::new(parse_required_expr(binary_expr.l)?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(parse_required_expr(binary_expr.r)?),
            })
        } else if self.has_column_index {
            Ok(Expr::Column(self.column_index as usize))
        } else if self.has_column_name {
            Ok(Expr::UnresolvedColumn(self.column_name))
        } else if self.has_literal_string {
            Ok(Expr::Literal(ScalarValue::Utf8(
                self.literal_string.clone(),
            )))
        } else if self.has_literal_double {
            Ok(Expr::Literal(ScalarValue::Float64(self.literal_double)))
        } else if self.has_literal_long {
            Ok(Expr::Literal(ScalarValue::Int64(self.literal_long)))
        } else if let Some(aggregate_expr) = self.aggregate_expr {
            let name = match aggregate_expr.aggr_function {
                f if f == protobuf::AggregateFunction::Min as i32 => Ok("MIN"),
                f if f == protobuf::AggregateFunction::Max as i32 => Ok("MAX"),
                f if f == protobuf::AggregateFunction::Sum as i32 => Ok("SUM"),
                f if f == protobuf::AggregateFunction::Avg as i32 => Ok("AVG"),
                f if f == protobuf::AggregateFunction::Count as i32 => Ok("COUNT"),
                other => Err(ballista_error(&format!(
                    "Unsupported aggregate function '{:?}'",
                    other
                ))),
            }?;

            Ok(Expr::AggregateFunction {
                name: name.to_owned(),
                args: vec![parse_required_expr(aggregate_expr.expr)?],
                return_type: DataType::Boolean, //TODO
            })
        } else {
            Err(ballista_error(&format!(
                "Unsupported logical expression '{:?}'",
                self
            )))
        }
    }
}

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        if self.query.is_some() {
            let plan: LogicalPlan = self.query.unwrap().try_into()?;
            Ok(Action::InteractiveQuery { plan })
        } else if self.task.is_some() {
            let task: ExecutionTask = self.task.unwrap().try_into()?;
            Ok(Action::Execute(task))
        } else {
            Err(BallistaError::NotImplemented(format!(
                "from_proto(Action) {:?}",
                self
            )))
        }
    }
}

fn from_proto_binary_op(op: &str) -> Result<Operator, BallistaError> {
    match op {
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        other => Err(ballista_error(&format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

fn from_proto_arrow_type(dt: i32) -> Result<DataType, BallistaError> {
    match dt {
        dt if dt == protobuf::ArrowType::Uint8 as i32 => Ok(DataType::UInt8),
        dt if dt == protobuf::ArrowType::Int8 as i32 => Ok(DataType::Int8),
        dt if dt == protobuf::ArrowType::Uint16 as i32 => Ok(DataType::UInt16),
        dt if dt == protobuf::ArrowType::Int16 as i32 => Ok(DataType::Int16),
        dt if dt == protobuf::ArrowType::Uint32 as i32 => Ok(DataType::UInt32),
        dt if dt == protobuf::ArrowType::Int32 as i32 => Ok(DataType::Int32),
        dt if dt == protobuf::ArrowType::Uint64 as i32 => Ok(DataType::UInt64),
        dt if dt == protobuf::ArrowType::Int64 as i32 => Ok(DataType::Int64),
        dt if dt == protobuf::ArrowType::Float as i32 => Ok(DataType::Float32),
        dt if dt == protobuf::ArrowType::Double as i32 => Ok(DataType::Float64),
        dt if dt == protobuf::ArrowType::Utf8 as i32 => Ok(DataType::Utf8),
        other => Err(BallistaError::General(format!(
            "Unsupported data type {:?}",
            other
        ))),
    }
}

impl TryInto<ExecutionTask> for protobuf::Task {
    type Error = BallistaError;

    fn try_into(self) -> Result<ExecutionTask, Self::Error> {
        //TODO
        //let x: Vec<ShuffleLocation> = self.shuffle_loc.iter().map(|s| s.try_into()).collect::<Result<_,_>>()?;

        Ok(ExecutionTask::new(
            Uuid::parse_str(&self.job_uuid).unwrap(),
            self.stage_id as usize,
            self.partition_id as usize,
            self.plan.unwrap().try_into()?,
            HashMap::new(),
        ))
    }
}

impl TryInto<ShuffleLocation> for protobuf::ShuffleLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<ShuffleLocation, Self::Error> {
        unimplemented!()
    }
}

impl TryInto<Schema> for protobuf::Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<Schema, Self::Error> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let dt: Result<DataType, _> = from_proto_arrow_type(c.arrow_type);
                dt.and_then(|dt| Ok(Field::new(&c.name, dt, c.nullable)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

impl TryInto<PhysicalPlan> for protobuf::PhysicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<PhysicalPlan, Self::Error> {
        if let Some(aggregate) = self.hash_aggregate {
            let input: PhysicalPlan = self.input.unwrap().as_ref().to_owned().try_into()?;
            let mode = match aggregate.mode {
                mode if mode == protobuf::AggregateMode::Partial as i32 => {
                    Ok(AggregateMode::Partial)
                }
                mode if mode == protobuf::AggregateMode::Final as i32 => Ok(AggregateMode::Final),
                mode if mode == protobuf::AggregateMode::Complete as i32 => {
                    Ok(AggregateMode::Complete)
                }
                other => Err(ballista_error(&format!(
                    "Unsupported aggregate mode '{}' for hash aggregate",
                    other
                ))),
            }?;
            let group_expr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_expr = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PhysicalPlan::HashAggregate(Arc::new(
                HashAggregateExec::try_new(mode, group_expr, aggr_expr, Arc::new(input))?,
            )))
        } else if let Some(scan) = self.scan {
            match scan.file_format.as_str() {
                "parquet" => Ok(PhysicalPlan::ParquetScan(Arc::new(
                    ParquetScanExec::try_new(
                        &scan.path,
                        Some(scan.projection.iter().map(|n| *n as usize).collect()),
                    )?,
                ))),
                other => Err(ballista_error(&format!(
                    "Unsupported file format '{}' for file scan",
                    other
                ))),
            }
        } else {
            Err(ballista_error(&format!(
                "Unsupported physical plan '{:?}'",
                self
            )))
        }
    }
}

fn parse_required_expr(p: Option<Box<protobuf::LogicalExprNode>>) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().to_owned().try_into(),
        None => Err(ballista_error("Missing required expression")),
    }
}
