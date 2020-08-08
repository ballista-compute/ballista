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

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::Result;
use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ExecutionContext, ExecutionPlan,
    Partitioning,
};

use crate::arrow::datatypes::Schema;
use crate::arrow::record_batch::RecordBatchReader;
use crate::parquet::arrow::arrow_reader::ArrowReader;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::parquet::arrow::ParquetFileArrowReader;
use crate::parquet::file::reader::SerializedFileReader;

use async_trait::async_trait;

/// ParquetScanExec reads Parquet files and applies an optional projection so that only necessary
/// columns are loaded into memory. The partitioning scheme is currently rather simplistic with a
/// one to one mapping of filename to partition. Also, there is currently no support for schema
/// merging, so all partitions must have the same schema.
#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    pub(crate) path: String,
    pub(crate) filenames: Vec<String>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) projected_fields: Vec<usize>,
    pub(crate) parquet_schema: Arc<Schema>,
    pub(crate) output_schema: Arc<Schema>,
    pub(crate) batch_size: usize,
}

impl ParquetScanExec {
    pub fn try_new(
        path: &str,
        filenames: Vec<String>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        println!("ParquetScanExec created with projection {:?}", projection);

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
            projected_fields,
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
            self.projected_fields.clone(),
            self.output_schema.clone(),
            self.batch_size,
        )?))
    }
}

pub struct ParquetBatchIter {
    schema: Arc<Schema>,
    batch_reader: Rc<RefCell<ParquetRecordBatchReader>>,
}

impl ParquetBatchIter {
    pub fn try_new(
        filename: &str,
        projection: Vec<usize>,
        projected_schema: Arc<Schema>,
        batch_size: usize,
    ) -> Result<Self> {
        println!("Reading {} with projection {:?}", filename, projection);

        let file = File::open(filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.clone(), batch_size)
            .unwrap();

        Ok(Self {
            schema: projected_schema,
            batch_reader: Rc::new(RefCell::new(batch_reader)),
        })
    }
}

impl ColumnarBatchIter for ParquetBatchIter {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&self) -> Result<Option<ColumnarBatch>> {
        let mut ref_mut = self.batch_reader.borrow_mut();
        match ref_mut.next_batch().unwrap() {
            Some(batch) => Ok(Some(ColumnarBatch::from_arrow(&batch))),
            None => Ok(None),
        }
    }
}
