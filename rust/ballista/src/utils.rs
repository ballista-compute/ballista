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

use std::sync::Arc;
use std::{fs::File, pin::Pin};

use crate::error::{BallistaError, Result};
use crate::memory_stream::MemoryStream;

use crate::scheduler::execution_plans::{QueryStageExec, UnresolvedShuffleExec};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::csv::CsvExec;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, RecordBatchStream};
use futures::StreamExt;

/// Summary of executed partition
#[derive(Debug, Copy, Clone)]
pub struct PartitionStats {
    num_rows: usize,
    num_batches: usize,
    num_bytes: usize,
    null_count: usize,
}

/// Stream data to disk in Arrow IPC format

pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send + Sync>>,
    path: &str,
) -> Result<PartitionStats> {
    let file = File::create(&path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to create partition file at {}: {:?}",
            path, e
        ))
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;
    let mut null_count = 0;
    let mut writer = FileWriter::try_new(file, stream.schema().as_ref())?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum();
        let batch_null_count: usize = batch.columns().iter().map(|array| array.null_count()).sum();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;
        null_count += batch_null_count;
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(PartitionStats {
        num_rows,
        num_batches,
        num_bytes,
        null_count,
    })
}

pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send + Sync>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn format_plan(plan: &dyn ExecutionPlan, indent: usize) -> Result<String> {
    let operator_str = if let Some(exec) = plan.as_any().downcast_ref::<HashAggregateExec>() {
        format!(
            "HashAggregateExec: groupBy={:?}, aggrExpr={:?}",
            exec.group_expr()
                .iter()
                .map(|e| format_expr(e.0.as_ref()))
                .collect::<Vec<String>>(),
            exec.aggr_expr()
                .iter()
                .map(|e| format_agg_expr(e.as_ref()))
                .collect::<Result<Vec<String>>>()?
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<HashJoinExec>() {
        format!(
            "HashJoinExec: joinType={:?}, on={:?}",
            exec.join_type(),
            exec.on()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
        format!("ParquetExec: partitions={}", exec.partitions().len())
    } else if let Some(exec) = plan.as_any().downcast_ref::<CsvExec>() {
        format!(
            "CsvExec: {}; partitions={}",
            &exec.path(),
            exec.output_partitioning().partition_count()
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        format!("FilterExec: {}", format_expr(exec.predicate().as_ref()))
    } else if let Some(exec) = plan.as_any().downcast_ref::<QueryStageExec>() {
        format!(
            "QueryStageExec: job={}, stage={}",
            exec.job_id, exec.stage_id
        )
    } else if let Some(exec) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        format!("UnresolvedShuffleExec: stages={:?}", exec.query_stage_ids)
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        format!(
            "CoalesceBatchesExec: batchSize={}",
            exec.target_batch_size()
        )
    } else if plan.as_any().downcast_ref::<MergeExec>().is_some() {
        "MergeExec".to_string()
    } else {
        let str = format!("{:?}", plan);
        String::from(&str[0..120])
    };
    Ok(format!(
        "{}{}\n{}",
        "  ".repeat(indent),
        &operator_str,
        plan.children()
            .iter()
            .map(|c| format_plan(c.as_ref(), indent + 1))
            .collect::<Result<Vec<String>>>()?
            .join("\n")
    ))
}

pub fn format_agg_expr(expr: &dyn AggregateExpr) -> Result<String> {
    Ok(format!(
        "{} {:?}",
        expr.field()?.name(),
        expr.expressions()
            .iter()
            .map(|e| format_expr(e.as_ref()))
            .collect::<Vec<String>>()
    ))
}

pub fn format_expr(expr: &dyn PhysicalExpr) -> String {
    if let Some(e) = expr.as_any().downcast_ref::<Column>() {
        e.name().to_string()
    } else if let Some(e) = expr.as_any().downcast_ref::<Literal>() {
        e.to_string()
    } else if let Some(e) = expr.as_any().downcast_ref::<BinaryExpr>() {
        format!("{} {} {}", e.left(), e.op(), e.right())
    } else {
        format!("{}", expr)
    }
}
