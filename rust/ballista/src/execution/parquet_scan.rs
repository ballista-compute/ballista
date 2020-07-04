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
use crate::parquet::arrow::arrow_reader::ArrowReader;
use crate::parquet::arrow::ParquetFileArrowReader;
use crate::parquet::file::reader::SerializedFileReader;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::task::{Context, Poll};
use tokio::stream::Stream;

type MaybeColumnarBatch = Result<Option<ColumnarBatch>>;

#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    paths: Vec<String>,
    projection: Option<Vec<usize>>,
}

impl ExecutionPlan for ParquetScanExec {
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.paths.len())
    }

    fn execute(&self, _partition_index: usize) -> Result<ColumnarBatchStream> {
        unimplemented!()
    }
}

struct ParquetStream {
    // schema: Arc<Schema>,
    request_tx: Sender<()>,
    response_rx: Receiver<Result<Option<ColumnarBatch>>>,
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
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = unbounded();
        let (response_tx, response_rx): (Sender<MaybeColumnarBatch>, Receiver<MaybeColumnarBatch>) =
            unbounded();

        let filename = filename.to_string();

        thread::spawn(move || {
            //TODO error handling, remove unwraps

            let batch_size = 64 * 1024; //TODO

            // open file
            let file = File::open(&filename).unwrap();
            match SerializedFileReader::new(file) {
                Ok(file_reader) => {
                    let file_reader = Rc::new(file_reader);

                    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);

                    match arrow_reader.get_record_reader_by_columns(projection, batch_size) {
                        Ok(mut batch_reader) => {
                            while request_rx.recv().is_ok() {
                                match batch_reader.next_batch() {
                                    Ok(Some(batch)) => {
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
                            }
                        }

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
        });

        println!("try_new ok");

        Ok(Self {
            // schema: projected_schema,
            request_tx,
            response_rx,
        })
    }
}

impl Stream for ParquetStream {
    type Item = ColumnarBatch;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("poll_next()");

        self.request_tx.send(()).unwrap();

        match self.response_rx.recv().unwrap().unwrap() {
            Some(batch) => {
                println!("ready");
                Poll::Ready(Some(batch))
            }
            _ => {
                println!("pending");
                Poll::Pending
            }
        }
    }
}
