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

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::serde::{empty_physical_plan_node, protobuf, BallistaError};

use datafusion::physical_plan::csv::CsvExec;
use datafusion::physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
    NegativeExpr, NotExpr,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::functions::ScalarFunctionExpr;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

impl TryInto<protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PhysicalPlanNode, Self::Error> {
        let plan = self.as_any();
        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let mut node = empty_physical_plan_node();
            node.input = Some(Box::new(input));
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>, Self::Error>>()?;
            node.projection = Some(protobuf::ProjectionExecNode { expr });
            Ok(node)
        } else if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let mut node = empty_physical_plan_node();
            node.input = Some(Box::new(input));
            //         node.selection = Some(protobuf::SelectionExecNode {
            //             expr: Some(exec.as_ref().filter_expr.as_ref().try_into()?),
            //         });
            Ok(node)
        } else if let Some(exec) = plan.downcast_ref::<HashAggregateExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let mut node = empty_physical_plan_node();
            node.input = Some(Box::new(input));
            //         node.hash_aggregate = Some(protobuf::HashAggregateExecNode {
            //             mode: match exec.mode {
            //                 AggregateMode::Partial => protobuf::AggregateMode::Partial,
            //                 AggregateMode::Final => protobuf::AggregateMode::Final,
            //                 AggregateMode::Complete => protobuf::AggregateMode::Complete,
            //             }
            //                 .into(),
            //             group_expr: exec
            //                 .group_expr
            //                 .iter()
            //                 .map(|expr| expr.try_into())
            //                 .collect::<Result<Vec<_>, BallistaError>>()?,
            //             aggr_expr: exec
            //                 .aggr_expr
            //                 .iter()
            //                 .map(|expr| expr.try_into())
            //                 .collect::<Result<Vec<_>, BallistaError>>()?,
            //         });
            Ok(node)
        } else if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
            let _left: protobuf::PhysicalPlanNode = exec.left().to_owned().try_into()?;
            let _right: protobuf::PhysicalPlanNode = exec.right().to_owned().try_into()?;
            let node = empty_physical_plan_node();
            Ok(node)
        } else if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let mut node = empty_physical_plan_node();
            node.input = Some(Box::new(input));
            Ok(node)
        } else if let Some(_exec) = plan.downcast_ref::<CsvExec>() {
            let node = empty_physical_plan_node();
            //         node.scan = Some(protobuf::ScanExecNode {
            //             path: exec.path.clone(),
            //             filename: exec.filenames.clone(),
            //             projection: exec
            //                 .projection
            //                 .as_ref()
            //                 .unwrap()
            //                 .iter()
            //                 .map(|n| *n as u32)
            //                 .collect(),
            //             file_format: "csv".to_owned(),
            //             schema: Some(exec.original_schema().as_ref().try_into()?),
            //             has_header: exec.has_header,
            //             batch_size: exec.batch_size as u32,
            //         });
            Ok(node)
        } else if let Some(_exec) = plan.downcast_ref::<ParquetExec>() {
            let node = empty_physical_plan_node();
            //         node.scan = Some(protobuf::ScanExecNode {
            //             path: exec.path.clone(),
            //             filename: exec.filenames.clone(),
            //             projection: exec
            //                 .projection
            //                 .as_ref()
            //                 .unwrap()
            //                 .iter()
            //                 .map(|n| *n as u32)
            //                 .collect(),
            //             file_format: "parquet".to_owned(),
            //             schema: Some(exec.parquet_schema.as_ref().try_into()?),
            //             has_header: false,
            //             batch_size: exec.batch_size as u32,
            //         });
            Ok(node)

        //     PhysicalPlan::ShuffleReader(exec) => {
        //         let mut node = empty_physical_plan_node();
        //
        //         let shuffle_id: Vec<protobuf::ShuffleId> = exec
        //             .shuffle_id
        //             .iter()
        //             .map(|s| s.try_into())
        //             .collect::<Result<_, _>>()?;
        //
        //         node.shuffle_reader = Some(protobuf::ShuffleReaderExecNode {
        //             schema: Some(exec.schema().as_ref().try_into()?),
        //             shuffle_id,
        //         });
        //         Ok(node)
        //     }
        } else {
            Err(BallistaError::General(format!(
                "physical plan to_proto {:?}",
                self
            )))
        }
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        let expr = value.as_any();
        if let Some(expr) = expr.downcast_ref::<Column>() {
            Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::ColumnName(
                    expr.name().to_owned(),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            let binary_expr = Box::new(protobuf::BinaryExprNode {
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
                op: format!("{:?}", expr.op()),
            });
            Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::BinaryExpr(
                    binary_expr,
                )),
            })
        } else if let Some(_expr) = expr.downcast_ref::<Literal>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<CaseExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<CastExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<NotExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<IsNullExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<IsNotNullExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<InListExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<InListExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<NegativeExpr>() {
            unimplemented!()
        } else if let Some(_expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
            unimplemented!()
        } else {
            Err(BallistaError::General(format!(
                "unsupported physical expression {:?}",
                value
            )))
        }
    }
}
