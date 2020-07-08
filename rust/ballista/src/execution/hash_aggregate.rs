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

//! Hash Aggregate operator. This is based on the implementation from DataFusion.

use std::rc::Rc;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
//use async_mutex::Mutex;

use crate::arrow::datatypes::Schema;
use crate::datafusion::logicalplan::Expr;
use crate::error::Result;
use crate::execution::physical_plan::{
    compile_expressions, AggregateMode, ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream,
    Distribution, ExecutionPlan, Expression, Partitioning, PhysicalPlan, AggregateExpr,
    compile_aggregate_expressions, Accumulator
};
use fnv::FnvHashMap;

#[allow(dead_code)]
#[derive(Debug)]
pub struct HashAggregateExec {
    pub(crate) mode: AggregateMode,
    pub(crate) group_expr: Vec<Arc<dyn Expression>>,
    pub(crate) aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    pub(crate) child: Rc<PhysicalPlan>,
}

impl HashAggregateExec {
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        child: Rc<PhysicalPlan>,
    ) -> Result<Self> {

        let group_expr = compile_expressions(&group_expr, &child)?;
        let aggr_expr = compile_aggregate_expressions(&aggr_expr, &child)?;

        Ok(Self {
            mode,
            group_expr,
            aggr_expr,
            child,
        })
    }

    pub fn with_new_children(&self, new_children: Vec<Rc<PhysicalPlan>>) -> HashAggregateExec {
        assert!(new_children.len() == 1);
        HashAggregateExec {
            mode: self.mode.clone(),
            group_expr: self.group_expr.clone(),
            aggr_expr: self.aggr_expr.clone(),
            child: new_children[0].clone(),
        }
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        match self.mode {
            AggregateMode::Partial => self.child.as_execution_plan().output_partitioning(),
            AggregateMode::Final => Partitioning::UnknownPartitioning(1),
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        match self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            AggregateMode::Final => Distribution::SinglePartition,
        }
    }

    fn children(&self) -> Vec<Rc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        let input = self.child.as_execution_plan().execute(partition_index)?;
        Ok(Arc::new(HashAggregateIter::new(
            input,
            self.group_expr.clone(),
            self.aggr_expr.clone(),
        )))
    }
}

struct HashAggregateIter {
    input: ColumnarBatchStream,
    group_expr: Vec<Arc<dyn Expression>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    state: Arc<Mutex<HashAggregateState>>,
}

impl HashAggregateIter {
    fn new(input: ColumnarBatchStream,
           group_expr: Vec<Arc<dyn Expression>>,
           aggr_expr: Vec<Arc<dyn AggregateExpr>>) -> Self {
        Self { input, group_expr, aggr_expr,
            state: Arc::new(Mutex::new(HashAggregateState::new())) }
    }
}

/// internal mutable state used during execution of a hash aggregate
struct HashAggregateState {
    /// Map of grouping values to accumulator sets
    accum_map: FnvHashMap<Vec<GroupByScalar>, AccumulatorSet>
}

impl HashAggregateState {
    fn new() -> Self {
        Self {
            accum_map: FnvHashMap::default()
        }
    }
}
type AccumulatorSet = Vec<Arc<Mutex<dyn Accumulator>>>;

#[async_trait]
impl ColumnarBatchIter for HashAggregateIter {
    async fn next(&self) -> Result<Option<ColumnarBatch>> {

        while let Some(batch) = self.input.next().await? {

            let mut state = self.state.lock().unwrap();

            // evaluate the grouping expressions against this batch
            let group_keys = self
                .group_expr
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<_>>>()?;

            // evaluate the inputs to the aggregate expressions for this batch
            let aggr_input_values = self
                .aggr_expr
                .iter()
                .map(|expr| expr.evaluate_input(&batch))
                .collect::<Result<Vec<_>>>()?;

            //TODO try doing loops of map lookups, then loops of accumulation as per the talk
            // from spark ai summit, to better leverage SIMD

            for i in 0..group_keys.len() {

                // create keys
                let keys: Vec<GroupByScalar> = vec![];

                // do map lookup
                let updated = match state.accum_map.get(&keys) {
                    Some(accum_set) => {
                        //TODO update accumulators
                        true
                    }
                    None => false
                };

                if !updated {
                    // create accumulators
                    let accumulators: Vec<Arc<Mutex<dyn Accumulator>>> = self
                        .aggr_expr
                        .iter()
                        .map(|expr| expr.create_accumulator())
                        .collect();

                    //TODO update accumulators

                    state.accum_map.insert(keys, accumulators);
                }
            }
        }

        //TODO return aggregate result
        Ok(None)
    }
}

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum GroupByScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}