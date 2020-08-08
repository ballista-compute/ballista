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

//! CSV scan operator. Forked from DataFusion.

use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::arrow::csv;
use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::datafusion::execution::physical_plan::csv::CsvReadOptions;
use crate::error::{ballista_error, Result};

use crate::execution::physical_plan::{
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ExecutionContext, ExecutionPlan,
    Partitioning,
};
use async_trait::async_trait;

/// Execution plan for scanning a CSV file
pub struct CsvScanExec {
    /// Path to directory containing partitioned CSV files with the same schema
    pub(crate) path: String,
    /// Individual files
    pub(crate) filenames: Vec<String>,
    /// Schema representing the CSV file
    schema: SchemaRef,
    /// Does the CSV file have a header?
    pub(crate) has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional projection for which columns to load
    pub(crate) projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    pub(crate) batch_size: usize,
}

impl CsvScanExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        path: &str,
        filenames: Vec<String>,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s.clone(),
            None => CsvScanExec::try_infer_schema(&filenames, &options)?,
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            path: path.to_string(),
            filenames,
            schema: Arc::new(schema),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
        })
    }

    /// Infer schema for given CSV dataset
    pub fn try_infer_schema(filenames: &[String], options: &CsvReadOptions) -> Result<Schema> {
        Ok(csv::infer_schema_from_files(
            &filenames,
            options.delimiter,
            Some(options.schema_infer_max_records),
            options.has_header,
        )?)
    }

    pub fn original_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for CsvScanExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    async fn execute(
        &self,
        _ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        Ok(Arc::new(CsvBatchIter::try_new(
            &self.filenames[partition_index],
            self.schema.clone(),
            self.has_header,
            self.delimiter,
            &self.projection,
            self.projected_schema.clone(),
            self.batch_size,
        )?))
    }
}

struct CsvBatchIter {
    /// Arrow CSV reader
    reader: Arc<Mutex<csv::Reader<File>>>,
    /// Schema after the projection has been applied
    schema: SchemaRef,
}

impl CsvBatchIter {
    /// Create an iterator for a CSV file
    pub fn try_new(
        filename: &str,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        projected_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let reader = csv::Reader::new(
            file,
            schema,
            has_header,
            delimiter,
            batch_size,
            projection.clone(),
        );

        Ok(Self {
            reader: Arc::new(Mutex::new(reader)),
            schema: projected_schema,
        })
    }
}

impl ColumnarBatchIter for CsvBatchIter {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&self) -> Result<Option<ColumnarBatch>> {
        let mut reader = self.reader.lock().expect("failed to lock mutex");
        match reader.next() {
            Ok(Some(batch)) => Ok(Some(ColumnarBatch::from_arrow(&batch))),
            Ok(None) => Ok(None),
            Err(e) => Err(ballista_error(&format!(
                "Error reading CSV: {}",
                e.to_string()
            ))),
        }
    }
}
