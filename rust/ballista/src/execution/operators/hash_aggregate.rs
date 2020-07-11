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

//! Ballista Hash Aggregate operator. This is based on the implementation from DataFusion in the
//! Apache Arrow project.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use crossbeam::channel::{unbounded, Receiver, Sender};
use fnv::FnvHashMap;
use smol::Task;

use crate::arrow::array::StringBuilder;
use crate::arrow::array::{self, ArrayRef};
use crate::arrow::datatypes::{DataType, Schema};
use crate::datafusion::logicalplan::{Expr, ScalarValue};
use crate::error::{BallistaError, Result};
use crate::execution::physical_plan::{
    compile_aggregate_expressions, compile_expressions, Accumulator, AggregateExpr, AggregateMode,
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue, Distribution,
    ExecutionPlan, Expression, MaybeColumnarBatch, Partitioning, PhysicalPlan,
};
use std::time::Instant;

#[allow(dead_code)]
#[derive(Debug)]
pub struct HashAggregateExec {
    pub(crate) mode: AggregateMode,
    pub(crate) group_expr: Vec<Expr>,
    pub(crate) aggr_expr: Vec<Expr>,
    pub(crate) child: Arc<PhysicalPlan>,
    schema: Arc<Schema>,
}

impl HashAggregateExec {
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        child: Arc<PhysicalPlan>,
    ) -> Result<Self> {
        //TODO should just use schema from logical plan rather than derive it here?

        let input_schema = child.as_execution_plan().schema();
        let compiled_group_expr = compile_expressions(&group_expr, &input_schema)?;
        let compiled_aggr_expr = compile_aggregate_expressions(&aggr_expr, &input_schema)?;

        let mut fields = compiled_group_expr
            .iter()
            .map(|e| e.to_schema_field(&input_schema))
            .collect::<Result<Vec<_>>>()?;
        fields.extend(
            compiled_aggr_expr
                .iter()
                .map(|e| e.to_schema_field(&input_schema))
                .collect::<Result<Vec<_>>>()?,
        );

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            mode,
            group_expr,
            aggr_expr,
            child,
            schema,
        })
    }

    pub fn with_new_children(&self, new_children: Vec<Arc<PhysicalPlan>>) -> HashAggregateExec {
        assert!(new_children.len() == 1);
        HashAggregateExec {
            mode: self.mode.clone(),
            group_expr: self.group_expr.clone(),
            aggr_expr: self.aggr_expr.clone(),
            child: new_children[0].clone(),
            schema: self.schema(),
        }
    }
}

impl ExecutionPlan for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        match self.mode {
            AggregateMode::Partial => self.child.as_execution_plan().output_partitioning(),
            _ => Partitioning::UnknownPartitioning(1),
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        match self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            _ => Distribution::SinglePartition,
        }
    }

    fn children(&self) -> Vec<Arc<PhysicalPlan>> {
        vec![self.child.clone()]
    }

    fn execute(&self, partition_index: usize) -> Result<ColumnarBatchStream> {
        let child_exec = self.child.as_execution_plan();
        let input_schema = child_exec.schema();
        let input = child_exec.execute(partition_index)?;
        let group_expr = compile_expressions(&self.group_expr, &input_schema)?;
        let aggr_expr = compile_aggregate_expressions(&self.aggr_expr, &input_schema)?;
        Ok(Arc::new(HashAggregateIter::new(
            &self.mode, input, group_expr, aggr_expr,
        )))
    }
}

/// Create array from `key` attribute in map entry (representing a grouping scalar value)
macro_rules! extract_group_val {
    ($BUILDER:ident, $TY:ident, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = array::$BUILDER::new($MAP.len());
        let mut err = false;
        for k in $MAP.keys() {
            match k.get($COL_INDEX) {
                Some(GroupByScalar::$TY(n)) => builder.append_value(*n).unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(BallistaError::General(
                "unexpected type when creating grouping array from aggregate map".to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// Create array from `value` attribute in map entry (representing an aggregate scalar
/// value)
macro_rules! extract_aggr_value {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = array::$BUILDER::new($MAP.len());
        let mut err = false;
        for v in $MAP.values() {
            match v[$COL_INDEX].as_ref().borrow().get_value()? {
                Some(ScalarValue::$TY(n)) => builder.append_value(n as $TY2).unwrap(),
                None => builder.append_null().unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(BallistaError::General(
                "unexpected type when creating aggregate array from aggregate map".to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// Create array from single accumulator value
// macro_rules! aggr_array_from_accumulator {
//     ($BUILDER:ident, $TY:ident, $TY2:ty, $VALUE:expr) => {{
//         let mut builder = $BUILDER::new(1);
//         match $VALUE {
//             Some(ScalarValue::$TY(n)) => {
//                 builder.append_value(n as $TY2)?;
//                 Ok(Arc::new(builder.finish()) as ArrayRef)
//             }
//             None => {
//                 builder.append_null()?;
//                 Ok(Arc::new(builder.finish()) as ArrayRef)
//             }
//             _ => Err(BallistaError::General(
//                 "unexpected type when creating aggregate array from aggregate map".to_string(),
//             )),
//         }
//     }};
// }

macro_rules! update_accumulators {
    ($ARRAY:ident, $ARRAY_TY:ident, $SCALAR_TY:expr, $COL:expr, $ACCUM:expr) => {{
        let primitive_array = $ARRAY.as_any().downcast_ref::<array::$ARRAY_TY>().unwrap();

        for row in 0..$ARRAY.len() {
            if $ARRAY.is_valid(row) {
                let value = $SCALAR_TY(primitive_array.value(row));
                let mut accum = $ACCUM[row][$COL].borrow_mut();
                accum.accumulate(&ColumnarValue::Scalar(Some(value), 1))?;
            }
        }
    }};
}

/// AccumularSet is the value in the hash map
type AccumulatorSet = Vec<Rc<RefCell<dyn Accumulator>>>;

#[allow(dead_code)]
struct HashAggregateIter {
    task: Task<Result<()>>,
    rx: Receiver<MaybeColumnarBatch>,
}

impl HashAggregateIter {
    fn new(
        mode: &AggregateMode,
        input: ColumnarBatchStream,
        group_expr: Vec<Arc<dyn Expression>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Self {
        let (tx, rx): (Sender<MaybeColumnarBatch>, Receiver<MaybeColumnarBatch>) = unbounded();

        let task = create_task(
            mode,
            input.clone(),
            group_expr.clone(),
            aggr_expr.clone(),
            tx,
        );
        Self { task, rx }
    }
}

fn create_task(
    mode: &AggregateMode,
    input: ColumnarBatchStream,
    group_expr: Vec<Arc<dyn Expression>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    tx: Sender<MaybeColumnarBatch>,
) -> Task<Result<()>> {
    let mode = mode.to_owned();

    Task::local(async move {
        let start = Instant::now();
        let mut batch_count = 0;
        let mut row_count = 0;

        // hash map of grouping values to accumulators
        let mut map: FnvHashMap<Vec<GroupByScalar>, AccumulatorSet> = FnvHashMap::default();

        // create vector large enough to hold the grouping key that can be re-used per row to
        // avoid the cost of creating a new vector each time
        let mut key = Vec::with_capacity(group_expr.len());
        for _ in 0..group_expr.len() {
            key.push(GroupByScalar::UInt32(0));
        }

        // iterate over all the input batches .. not that in partial mode it would be valid
        // to emit batches periodically and reset the accumlator map to reduce memory pressure
        while let Some(batch) = input.next().await? {
            batch_count += 1;
            row_count += batch.num_rows();

            // evaluate the grouping expressions against this batch
            let group_values = group_expr
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<_>>>()?;

            // iterate over each row in the batch and create the accumulators for each grouping key
            let mut accumulators: Vec<AccumulatorSet> = Vec::with_capacity(batch.num_rows());

            for row in 0..batch.num_rows() {
                // create grouping key for this row
                create_key(&group_values, row, &mut key)?;

                if let Some(accumulator_set) = map.get(&key) {
                    accumulators.push(accumulator_set.clone());
                } else {
                    let accumulator_set: AccumulatorSet = aggr_expr
                        .iter()
                        .map(|expr| expr.create_accumulator(&mode))
                        .collect();

                    map.insert(key.clone(), accumulator_set.clone());
                    accumulators.push(accumulator_set);
                }
            }

            // iterate over each aggregate expression
            for (col, expr) in aggr_expr.iter().enumerate() {
                // evaluate the aggregate expression
                let value = expr.evaluate_input(&batch)?;

                match &value {
                    ColumnarValue::Columnar(array) => match array.data_type() {
                        DataType::Int8 => update_accumulators!(
                            array,
                            Int8Array,
                            ScalarValue::Int8,
                            col,
                            accumulators
                        ),
                        DataType::Int16 => update_accumulators!(
                            array,
                            Int16Array,
                            ScalarValue::Int16,
                            col,
                            accumulators
                        ),
                        DataType::Int32 => update_accumulators!(
                            array,
                            Int32Array,
                            ScalarValue::Int32,
                            col,
                            accumulators
                        ),
                        DataType::Int64 => update_accumulators!(
                            array,
                            Int64Array,
                            ScalarValue::Int64,
                            col,
                            accumulators
                        ),
                        DataType::UInt8 => update_accumulators!(
                            array,
                            UInt8Array,
                            ScalarValue::UInt8,
                            col,
                            accumulators
                        ),
                        DataType::UInt16 => update_accumulators!(
                            array,
                            UInt16Array,
                            ScalarValue::UInt16,
                            col,
                            accumulators
                        ),
                        DataType::UInt32 => update_accumulators!(
                            array,
                            UInt32Array,
                            ScalarValue::UInt32,
                            col,
                            accumulators
                        ),
                        DataType::UInt64 => update_accumulators!(
                            array,
                            UInt64Array,
                            ScalarValue::UInt64,
                            col,
                            accumulators
                        ),
                        DataType::Float32 => update_accumulators!(
                            array,
                            Float32Array,
                            ScalarValue::Float32,
                            col,
                            accumulators
                        ),
                        DataType::Float64 => update_accumulators!(
                            array,
                            Float64Array,
                            ScalarValue::Float64,
                            col,
                            accumulators
                        ),
                        other => {
                            return Err(BallistaError::General(format!(
                                "Unsupported data type {:?} for result of aggregate expression",
                                other
                            )));
                        }
                    },
                    _ => unimplemented!(),
                };
            }
        }

        // prepare results
        let batch = create_batch_from_accum_map(
            &map,
            input.as_ref().schema().as_ref(),
            &group_expr,
            &aggr_expr,
        )?;

        // send the result batch over the channel
        tx.send(Ok(Some(batch))).unwrap();

        // send EOF marker
        tx.send(Ok(None)).unwrap();

        println!(
            "HashAggregate processed {} batches and {} rows in {} ms",
            batch_count,
            row_count,
            start.elapsed().as_millis()
        );

        Ok(())
    })
}

/// Create a Vec<GroupByScalar> that can be used as a map key
fn create_key(
    group_by_keys: &[ColumnarValue],
    row: usize,
    vec: &mut Vec<GroupByScalar>,
) -> Result<()> {
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col {
            ColumnarValue::Columnar(col) => match col.data_type() {
                DataType::UInt8 => {
                    let array = col.as_any().downcast_ref::<array::UInt8Array>().unwrap();
                    vec[i] = GroupByScalar::UInt8(array.value(row))
                }
                DataType::UInt16 => {
                    let array = col.as_any().downcast_ref::<array::UInt16Array>().unwrap();
                    vec[i] = GroupByScalar::UInt16(array.value(row))
                }
                DataType::UInt32 => {
                    let array = col.as_any().downcast_ref::<array::UInt32Array>().unwrap();
                    vec[i] = GroupByScalar::UInt32(array.value(row))
                }
                DataType::UInt64 => {
                    let array = col.as_any().downcast_ref::<array::UInt64Array>().unwrap();
                    vec[i] = GroupByScalar::UInt64(array.value(row))
                }
                DataType::Int8 => {
                    let array = col.as_any().downcast_ref::<array::Int8Array>().unwrap();
                    vec[i] = GroupByScalar::Int8(array.value(row))
                }
                DataType::Int16 => {
                    let array = col.as_any().downcast_ref::<array::Int16Array>().unwrap();
                    vec[i] = GroupByScalar::Int16(array.value(row))
                }
                DataType::Int32 => {
                    let array = col.as_any().downcast_ref::<array::Int32Array>().unwrap();
                    vec[i] = GroupByScalar::Int32(array.value(row))
                }
                DataType::Int64 => {
                    let array = col.as_any().downcast_ref::<array::Int64Array>().unwrap();
                    vec[i] = GroupByScalar::Int64(array.value(row))
                }
                DataType::Utf8 => {
                    let array = col.as_any().downcast_ref::<array::StringArray>().unwrap();
                    vec[i] = GroupByScalar::Utf8(String::from(array.value(row)))
                }
                _ => {
                    return Err(BallistaError::General(
                        "Unsupported GROUP BY data type".to_string(),
                    ))
                }
            },
            _ => unimplemented!(),
        }
    }
    Ok(())
}

/// Create a columnar batch from the hash map
fn create_batch_from_accum_map(
    map: &FnvHashMap<Vec<GroupByScalar>, AccumulatorSet>,
    input_schema: &Schema,
    group_expr: &[Arc<dyn Expression>],
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<ColumnarBatch> {
    // build the result arrays
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(group_expr.len() + aggr_expr.len());

    // grouping values
    for i in 0..group_expr.len() {
        let data_type = group_expr[i].data_type(&input_schema)?;
        let array = match data_type {
            DataType::UInt8 => extract_group_val!(UInt8Builder, UInt8, map, i),
            DataType::UInt16 => extract_group_val!(UInt16Builder, UInt16, map, i),
            DataType::UInt32 => extract_group_val!(UInt32Builder, UInt32, map, i),
            DataType::UInt64 => extract_group_val!(UInt64Builder, UInt64, map, i),
            DataType::Int8 => extract_group_val!(Int8Builder, Int8, map, i),
            DataType::Int16 => extract_group_val!(Int16Builder, Int16, map, i),
            DataType::Int32 => extract_group_val!(Int32Builder, Int32, map, i),
            DataType::Int64 => extract_group_val!(Int64Builder, Int64, map, i),
            DataType::Utf8 => {
                let mut builder = StringBuilder::new(1);
                for k in map.keys() {
                    match &k[i] {
                        GroupByScalar::Utf8(s) => builder.append_value(&s).unwrap(),
                        _ => {
                            return Err(BallistaError::General(
                                "Unexpected value for Utf8 group column".to_string(),
                            ))
                        }
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            _ => Err(BallistaError::General(
                "Unsupported group by expr".to_string(),
            )),
        };
        arrays.push(array?);
    }

    // aggregate values
    for i in 0..aggr_expr.len() {
        let data_type = aggr_expr[i].data_type(&input_schema)?;
        let array = match data_type {
            DataType::UInt8 => extract_aggr_value!(UInt64Builder, UInt8, u64, map, i),
            DataType::UInt16 => extract_aggr_value!(UInt64Builder, UInt16, u64, map, i),
            DataType::UInt32 => extract_aggr_value!(UInt64Builder, UInt32, u64, map, i),
            DataType::UInt64 => extract_aggr_value!(UInt64Builder, UInt64, u64, map, i),
            DataType::Int8 => extract_aggr_value!(Int64Builder, Int8, i64, map, i),
            DataType::Int16 => extract_aggr_value!(Int64Builder, Int16, i64, map, i),
            DataType::Int32 => extract_aggr_value!(Int64Builder, Int32, i64, map, i),
            DataType::Int64 => extract_aggr_value!(Int64Builder, Int64, i64, map, i),
            DataType::Float32 => extract_aggr_value!(Float32Builder, Float32, f32, map, i),
            DataType::Float64 => extract_aggr_value!(Float64Builder, Float64, f64, map, i),
            _ => Err(BallistaError::General(
                "Unsupported aggregate expr".to_string(),
            )),
        };
        arrays.push(array?);
    }

    let values: Vec<ColumnarValue> = arrays
        .iter()
        .map(|a| ColumnarValue::Columnar(a.clone()))
        .collect();

    Ok(ColumnarBatch::from_values(&values))
}

#[async_trait]
impl ColumnarBatchIter for HashAggregateIter {
    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    async fn next(&self) -> Result<Option<ColumnarBatch>> {
        let channel = self.rx.clone();
        Task::blocking(async move {
            channel
                .recv()
                .map_err(|e| BallistaError::General(format!("{:?}", e.to_string())))?
        })
        .await
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
