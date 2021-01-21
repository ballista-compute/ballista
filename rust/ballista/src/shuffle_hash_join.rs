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

//! The ShuffleHashJoinExec operator accepts two inputs that are hash-partitioned on join keys so
//! that each partition of the Shuffle Hash Join operates on the corresponding partitions of the
//! two inputs
//!
//! This operator is EXPERIMENTAL and still under development

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{error::{DataFusionError, Result},
                 physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream}};

#[derive(Debug, Clone)]

pub struct ShuffleHashJoinExec {
    left:  Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl ShuffleHashJoinExec {
    /// Create a new ShuffleHashJoinExec and validate that both inputs are using hash partitioning
    /// with compatible expressions

    pub fn try_new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Result<Self> {

        match (left.output_partitioning(), right.output_partitioning()) {
            (Partitioning::Hash(left_expr, left_n), Partitioning::Hash(right_expr, right_n)) => {
                if left_n == 0 {

                    Err(DataFusionError::Plan(
                        "Both inputs to ShuffleHashJoinExec must have at least one hash expression".to_owned(),
                    ))
                } else if left_n != right_n {

                    Err(DataFusionError::Plan(
                        "Both inputs to ShuffleHashJoinExec must have same number of partitions".to_owned(),
                    ))
                } else {

                    let left_types = left_expr.iter().map(|e| e.data_type(left.schema().as_ref())).collect::<Result<Vec<_>>>()?;

                    let right_types = right_expr.iter().map(|e| e.data_type(right.schema().as_ref())).collect::<Result<Vec<_>>>()?;

                    if left_types == right_types {

                        Ok(Self { left, right })
                    } else {

                        Err(DataFusionError::Plan(
                            "Both inputs to ShuffleHashJoinExec must be partitioned with \
                            compatible hash expressions"
                                .to_owned(),
                        ))
                    }
                }
            },
            _ => Err(DataFusionError::Plan(
                "Both inputs to ShuffleHashJoinExec must have hash partitioning".to_owned(),
            )),
        }
    }
}

#[async_trait]

impl ExecutionPlan for ShuffleHashJoinExec {
    fn as_any(&self) -> &dyn Any {

        self
    }

    fn schema(&self) -> SchemaRef {

        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {

        self.right.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {

        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(&self, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {

        todo!()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {

        let _left = self.left.execute(partition).await?;

        let _right = self.right.execute(partition).await?;

        //TODO delegate to DataFusion HashJoinExec logic

        todo!()
    }
}

#[cfg(test)]

mod tests {

    use super::*;
    use arrow::{array::Int32Array,
                datatypes::{DataType, Field, Schema},
                record_batch::RecordBatch};
    use datafusion::physical_plan::memory::MemoryExec;

    #[test]

    fn input_not_hash_partitioned() -> Result<()> {

        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;

        let partitions = vec![vec![batch.clone()], vec![batch]];

        let mem_table: Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(&partitions, schema, None)?);

        let result = ShuffleHashJoinExec::try_new(mem_table.clone(), mem_table.clone());

        assert!(result.is_err());

        Ok(())
    }
}
