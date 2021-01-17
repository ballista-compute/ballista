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

//! The ShuffleWriteExec operator simply executes one partition of a physical plan, optionally
//! repartitions the output, and streams each output partition to disk for later retrieval by
//! future query stages.
//!
//! This operator is EXPERIMENTAL and still under development

use std::any::Any;

use crate::memory_stream::MemoryStream;
use crate::utils::write_stream_to_disk;

use arrow::array::{ArrayRef, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use tonic::codegen::Arc;

/// Shuffle write operator
#[derive(Debug)]
pub struct ShuffleWriteExec {
    /// The child plan to execute and write shuffle partitions for
    child: Arc<dyn ExecutionPlan>,
    /// Partition to execute
    child_partition: usize,
    /// Output path for shuffle partitions
    output_path: String,
    /// Number of shuffle partitions to crate
    target_partition_count: usize,
}

impl ShuffleWriteExec {
    /// Create a new ShuffleWriteExec
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        child_partition: usize,
        output_path: &str,
        target_partition_count: usize,
    ) -> Self {
        Self {
            child,
            child_partition,
            output_path: output_path.to_owned(),
            target_partition_count,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // The output of this operator is a single partition containing shuffle ids - it does
        // not directly output data
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "Ballista ShuffleWriteExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        assert!(partition == 0);

        let num_partitions = self.child.output_partitioning().partition_count();
        let mut partition_id = UInt32Builder::new(num_partitions);
        let mut partition_location =
            StringBuilder::with_capacity(num_partitions, num_partitions * 256);

        // execute input partition
        let mut stream = self.child.execute(self.child_partition).await?;

        //TODO apply partitioning - for now we just write out a single shuffle partition (which
        // is just the raw output from the executed plan's partition)
        //
        // When performing a shuffle as an input to a ShuffleHashJoin we will want to use hash
        // partitioning. In other cases we can just repartition using round-robin with the goal
        // of increasing parallelism in future query stages

        // stream data to disk in IPC format
        let shuffle_partition = 0;
        let path = format!("{}/{}", self.output_path, shuffle_partition);
        write_stream_to_disk(&mut stream, &path).await?;

        partition_id.append_value(shuffle_partition as u32)?;
        partition_location.append_value(&path)?;

        let partition_id: ArrayRef = Arc::new(partition_id.finish());
        let partition_location: ArrayRef = Arc::new(partition_location.finish());

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("partition_id", DataType::UInt32, false),
            Field::new("partition_location", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![partition_id, partition_location])?;
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryExec;
    use futures::stream::StreamExt;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let partitions = vec![vec![batch.clone()], vec![batch]];
        let mem_table: Arc<dyn ExecutionPlan> =
            Arc::new(MemoryExec::try_new(&partitions, schema, None)?);

        let tmp_dir = TempDir::new()?;

        let shuffle_write_exec =
            ShuffleWriteExec::new(mem_table, 0, tmp_dir.path().to_str().unwrap(), 200);
        assert_eq!(
            1,
            shuffle_write_exec.output_partitioning().partition_count()
        );

        let mut stream = shuffle_write_exec.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(1, batch.num_rows());
        assert!(stream.next().await.is_none());

        Ok(())
    }
}
