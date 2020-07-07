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

use std::fs::File;
use std::pin::Pin;
use std::rc::Rc;
use std::thread;

use crate::error::{BallistaError, Result};
use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchStream, ExecutionPlan, Partitioning,
};

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatchReader;
use crate::datafusion::execution::physical_plan::common;
use crate::parquet::arrow::arrow_reader::ArrowReader;
use crate::parquet::arrow::ParquetFileArrowReader;
use crate::parquet::file::reader::SerializedFileReader;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::task::{Context, Poll};
use tokio::stream::Stream;

type MaybeColumnarBatch = Option<Result<ColumnarBatch>>;

#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    pub(crate) path: String,
    pub(crate) filenames: Vec<String>,
    projection: Option<Vec<usize>>,
}

impl ParquetScanExec {
    pub fn try_new(path: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        Ok(Self {
            path: path.to_owned(),
            filenames,
            projection,
        })
    }
}

impl ExecutionPlan for ParquetScanExec {
    fn output_partitioning(&self) -> Partitioning {
        // note that this one partition per file which is crude and later we should support
        // splitting files into partitions as well
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        let stream =
            ParquetStream::try_new(&self.filenames[partition_index], self.projection.clone())?;
        Ok(Box::pin(stream))
    }
}

pub struct ParquetStream {
    // schema: Arc<Schema>,
    response_rx: Receiver<MaybeColumnarBatch>,
}

#[allow(dead_code)]
impl ParquetStream {
    pub fn try_new(filename: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let file = File::open(filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let schema = arrow_reader.get_schema().unwrap(); //TODO error handling

        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let _projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (Sender<MaybeColumnarBatch>, Receiver<MaybeColumnarBatch>) =
            unbounded();

        let filename = filename.to_string();

        thread::spawn(move || {
            //TODO error handling, remove unwraps
            let batch_size = 64 * 1024; //TODO
            let file = File::open(&filename).unwrap();
            match SerializedFileReader::new(file) {
                Ok(file_reader) => {
                    let file_reader = Rc::new(file_reader);
                    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                    match arrow_reader.get_record_reader_by_columns(projection, batch_size) {
                        Ok(mut batch_reader) => loop {
                            println!("reading batch");
                            match batch_reader.next_batch() {
                                Ok(Some(batch)) => {
                                    println!("sending batch");
                                    response_tx
                                        .send(Some(Ok(ColumnarBatch::from_arrow(&batch))))
                                        .unwrap();
                                }
                                Ok(None) => {
                                    println!("sending eof");
                                    response_tx.send(None).unwrap();
                                    break;
                                }
                                Err(e) => {
                                    println!("sending error");
                                    response_tx
                                        .send(Some(Err(BallistaError::General(format!("{:?}", e)))))
                                        .unwrap();
                                    break;
                                }
                            }
                        },

                        Err(e) => {
                            response_tx
                                .send(Some(Err(BallistaError::General(format!("{:?}", e)))))
                                .unwrap();
                        }
                    }
                }

                Err(e) => {
                    response_tx
                        .send(Some(Err(BallistaError::General(format!("{:?}", e)))))
                        .unwrap();
                }
            }
        });

        Ok(Self { response_rx })
    }
}

impl Stream for ParquetStream {
    type Item = Result<ColumnarBatch>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.response_rx.try_recv() {
            Ok(item) => Poll::Ready(item),
            Err(_) => {
                // this isn't efficient but it works
                ctx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
