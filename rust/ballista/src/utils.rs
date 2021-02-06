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

use std::fs::File;

use crate::memory_stream::MemoryStream;

use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use datafusion::physical_plan::SendableRecordBatchStream;
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
    stream: &mut SendableRecordBatchStream,
    path: &str,
) -> Result<PartitionStats> {
    let file = File::create(&path)?;
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

pub async fn read_stream_from_disk(path: &str) -> Result<SendableRecordBatchStream> {
    let file = File::open(&path)?;
    let reader = FileReader::try_new(file)?;
    let schema = reader.schema();
    // TODO we should be able return a stream / iterator rather than load into memory first
    let mut batches = vec![];
    for batch in reader {
        batches.push(batch?);
    }
    Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
}
