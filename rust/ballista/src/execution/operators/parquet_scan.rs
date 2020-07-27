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

//! Parquet scan operator.

use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{BallistaError, Result};
use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ExecutionContext, ExecutionPlan,
    MaybeColumnarBatch, Partitioning,
};

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatchReader;
use crate::datafusion::execution::physical_plan::common;
use crate::parquet::arrow::arrow_reader::ArrowReader;
use crate::parquet::arrow::ParquetFileArrowReader;
use crate::parquet::file::reader::SerializedFileReader;

use async_executor::Task;
use async_trait::async_trait;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::time::Instant;

/// ParquetScanExec reads Parquet files and applies an optional projection so that only necessary
/// columns are loaded into memory. The partitioning scheme is currently rather simplistic with a
/// one to one mapping of filename to partition. Also, there is currently no support for schema
/// merging, so all partitions must have the same schema.
#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    pub(crate) path: String,
    pub(crate) filenames: Vec<String>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) parquet_schema: Arc<Schema>,
    pub(crate) output_schema: Arc<Schema>,
    pub(crate) batch_size: usize,
}

impl ParquetScanExec {
    pub fn try_new(path: &str, projection: Option<Vec<usize>>, batch_size: usize) -> Result<Self> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        let filename = &filenames[0];
        let file = File::open(filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let schema = arrow_reader.get_schema().unwrap(); //TODO error handling

        let projected_fields = match &projection {
            Some(p) => p.clone(),
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projected_fields
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        Ok(Self {
            path: path.to_owned(),
            filenames,
            projection,
            parquet_schema: Arc::new(schema),
            output_schema: Arc::new(projected_schema),
            batch_size,
        })
    }
}

#[async_trait]
impl ExecutionPlan for ParquetScanExec {
    fn schema(&self) -> Arc<Schema> {
        self.output_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // note that this one partition per file which is crude and later we should support
        // splitting files into partitions as well
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    async fn execute(
        &self,
        _ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        Ok(Arc::new(ParquetBatchIter::try_new(
            &self.filenames[partition_index],
            self.projection.clone(),
            self.batch_size,
        )?))
    }
}

pub struct ParquetBatchIter {
    schema: Arc<Schema>,
    pub response_rx: Receiver<MaybeColumnarBatch>,
}

#[allow(dead_code)]
impl ParquetBatchIter {
    pub fn try_new(
        filename: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let schema = arrow_reader.get_schema().unwrap(); //TODO error handling

        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        let (response_tx, response_rx): (Sender<MaybeColumnarBatch>, Receiver<MaybeColumnarBatch>) =
            unbounded();

        let filename = filename.to_string();

        std::thread::spawn(move || {
            let start = Instant::now();
            let mut batch_read_time = 0;
            let mut output_batches = 0;
            let mut output_rows = 0;

            //TODO error handling, remove unwraps
            let file = File::open(&filename).unwrap();
            match SerializedFileReader::new(file) {
                Ok(file_reader) => {
                    let file_reader = Rc::new(file_reader);
                    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                    match arrow_reader.get_record_reader_by_columns(projection, batch_size) {
                        Ok(mut batch_reader) => loop {
                            // read the next batch
                            let start_batch = Instant::now();
                            let maybe_batch = batch_reader.next_batch();
                            batch_read_time += start_batch.elapsed().as_millis();

                            match maybe_batch {
                                Ok(Some(batch)) => {
                                    output_batches += 1;
                                    output_rows += batch.num_rows();

                                    response_tx
                                        .send(Ok(Some(ColumnarBatch::from_arrow(&batch))))
                                        .unwrap();
                                }
                                Ok(None) => {
                                    response_tx.send(Ok(None)).unwrap();
                                    break;
                                }
                                Err(e) => {
                                    response_tx
                                        .send(Err(BallistaError::General(format!("{:?}", e))))
                                        .unwrap();
                                    break;
                                }
                            }
                        },

                        Err(e) => {
                            response_tx
                                .send(Err(BallistaError::General(format!("{:?}", e))))
                                .unwrap();
                        }
                    }
                }

                Err(e) => {
                    response_tx
                        .send(Err(BallistaError::General(format!("{:?}", e))))
                        .unwrap();
                }
            }

            println!(
                "ParquetScan scanned {} batches and {} rows in {} ms. Total duration {} ms.",
                output_batches,
                output_rows,
                batch_read_time,
                start.elapsed().as_millis()
            );
        });

        Ok(Self {
            schema: Arc::new(projected_schema),
            response_rx,
        })
    }
}

#[async_trait]
impl ColumnarBatchIter for ParquetBatchIter {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        let channel = self.response_rx.clone();
        Task::spawn(async move { channel.recv().unwrap() }).await
    }
}
