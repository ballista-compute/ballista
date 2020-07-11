// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::arrow::datatypes::Schema;
use crate::error::Result;
use crate::execution::physical_plan::{ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ExecutionPlan, Partitioning, ExecutionContext};

use async_trait::async_trait;

pub struct InMemoryTableScanExec {
    data: Vec<Vec<ColumnarBatch>>,
}

impl InMemoryTableScanExec {
    pub fn new(data: Vec<Vec<ColumnarBatch>>) -> Self {
        Self { data }
    }
}

impl ExecutionPlan for InMemoryTableScanExec {
    fn schema(&self) -> Arc<Schema> {
        //TODO assumes some data exists
        self.data[0][0].schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.data.len())
    }

    fn execute(&self, _ctx: Arc<dyn ExecutionContext>, partition_index: usize) -> Result<ColumnarBatchStream> {
        let partition = &self.data[partition_index];
        Ok(Arc::new(InMemoryTableScanIter::new(partition)))
    }
}

pub struct InMemoryTableScanIter {
    index: Arc<AtomicUsize>,
    data: Vec<ColumnarBatch>,
}

impl InMemoryTableScanIter {
    fn new(data: &Vec<ColumnarBatch>) -> Self {
        Self {
            index: Arc::new(AtomicUsize::new(0)),
            data: data.clone(),
        }
    }
}

#[async_trait]
impl ColumnarBatchIter for InMemoryTableScanIter {
    fn schema(&self) -> Arc<Schema> {
        self.data[0].schema()
    }

    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        let index = self.index.load(Ordering::SeqCst);
        if index < self.data.len() {
            self.index.store(index + 1, Ordering::SeqCst);
            Ok(Some(self.data[index].clone()))
        } else {
            Ok(None)
        }
    }
}
