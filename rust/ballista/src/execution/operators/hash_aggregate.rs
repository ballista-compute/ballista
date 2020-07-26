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

use std::sync::Arc;
use std::time::Instant;

use crate::arrow::array::StringBuilder;
use crate::arrow::array::{self, ArrayRef};
use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::cast_array;
use crate::datafusion::logicalplan::{Expr, ScalarValue};
use crate::error::{ballista_error, BallistaError, Result};
use crate::execution::physical_plan::{
    compile_aggregate_expressions, compile_expressions, Accumulator, AggregateExpr, AggregateMode,
    ColumnarBatch, ColumnarBatchIter, ColumnarBatchStream, ColumnarValue, Distribution,
    ExecutionContext, ExecutionPlan, Expression, MaybeColumnarBatch, Partitioning, PhysicalPlan, compile_expression, compile_aggregate_expression,
};

use async_trait::async_trait;
use crossbeam::channel::{unbounded, Receiver, Sender};
use smol::Task;
use std::collections::HashMap;
use arrow::record_batch::RecordBatch;

/// HashAggregateExec applies a hash aggregate operation against its input.
#[derive(Debug)]
pub struct HashAggregateExec {
    pub(crate) mode: AggregateMode,
    // these are logical expressions because physical expressions
    // are not serializable due to their dynamic nature
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

        // compile the logical expressions along with their name
        let physical_group_expr: Vec<(Arc<dyn Expression>, String)> = group_expr.iter().map(|expr| {
            Ok((compile_expression(expr, &child.as_execution_plan().schema())?, expr.name(&input_schema)?.clone()))
        }).collect::<Result<Vec<_>>>()?;
        let physical_aggr_expr: Vec<(Arc<dyn AggregateExpr>, String)> = aggr_expr.iter().map(|expr| {
            Ok((compile_aggregate_expression(expr, &child.as_execution_plan().schema())?, expr.name(&input_schema)?.clone()))
        }).collect::<Result<Vec<_>>>()?;

        // build the output schema based on the physical expressions
        let mut fields = Vec::with_capacity(physical_group_expr.len() + physical_aggr_expr.len());
        for (expr, name) in &physical_group_expr {
            fields.push(Field::new(name, expr.data_type(&input_schema)?, expr.nullable(&input_schema)?));
        }
        for (expr, name) in &physical_aggr_expr {
            fields.push(Field::new(name, expr.data_type(&input_schema)?, expr.nullable(&input_schema)?));
        }
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

#[async_trait]
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

    async fn execute(
        &self,
        ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream> {
        let child_exec = self.child.as_execution_plan();
        let input_schema = child_exec.schema();
        let input = child_exec.execute(ctx.clone(), partition_index).await?;

        let group_expr = compile_expressions(&self.group_expr, &input_schema)?;
        let aggr_expr = compile_aggregate_expressions(&self.aggr_expr, &input_schema)?;

        let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
        for (i, expr) in group_expr.iter().enumerate() {
            fields.push(Field::new(&self.group_expr[i].name(&input_schema)?, expr.data_type(&input_schema)?, expr.nullable(&input_schema)?))
        }
        for (i, expr) in aggr_expr.iter().enumerate() {
            fields.push(Field::new(&self.aggr_expr[i].name(&input_schema)?, expr.data_type(&input_schema)?, expr.nullable(&input_schema)?))
        }
        let output_schema = Arc::new(Schema::new(fields));

        Ok(Arc::new(HashAggregateIter::new(
            &self.mode,
            input,
            group_expr,
            aggr_expr,
            input_schema,
            output_schema,
        )))
    }
}

/// Create array from `value` attribute in map entry (representing an aggregate scalar
/// value)
macro_rules! extract_aggr_value {
    ($BUILDER:ident, $TY:ident, $TY2:ty, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = array::$BUILDER::new($MAP.len());
        for v in $MAP.values() {
            match v[$COL_INDEX].as_ref().get_value()? {
                None => builder.append_null()?,
                Some(ScalarValue::$TY(n)) => builder.append_value(n as $TY2)?,
                Some(other) => {
                    return Err(ballista_error(&format!(
                        "Unexpected value {:?} for aggregate expr #{}",
                        other, $COL_INDEX
                    )))
                }
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Create array from `key` attribute in map entry (representing a grouping scalar value)
macro_rules! extract_group_val {
    ($BUILDER:ident, $TY:ident, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = array::$BUILDER::new($MAP.len());
        for k in $MAP.keys() {
            match k.get($COL_INDEX) {
                None => builder.append_null()?,
                Some(GroupByScalar::$TY(n)) => builder.append_value(*n)?,
                Some(other) => {
                    return Err(ballista_error(&format!(
                        "Unexpected value {:?} for group expr #{}",
                        other, $COL_INDEX
                    )))
                }
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
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
    ($ARRAY:ident, $ARRAY_TY:ident, $SCALAR_TY:expr, $ROW:expr, $COL:expr, $ACCUM:expr) => {{
        let primitive_array = cast_array!($ARRAY, $ARRAY_TY)?;
        if $ARRAY.is_valid($ROW) {
            let value = $SCALAR_TY(primitive_array.value($ROW));
            $ACCUM[$COL].accumulate(&ColumnarValue::Scalar(Some(value), 1))?;
        }
    }};
}

/// AccumularSet is the value in the hash map
type AccumulatorSet = Vec<Box<dyn Accumulator>>;

#[allow(dead_code)]
struct HashAggregateIter {
    schema: Arc<Schema>,
    rx: Receiver<MaybeColumnarBatch>,
}

fn run(
    tx: Sender<MaybeColumnarBatch>,
    mode: &AggregateMode,
    input: ColumnarBatchStream,
    group_expr: Vec<Arc<dyn Expression>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    input_schema: &Schema,
    output_schema: &Schema,
) -> Result<()> {
    smol::run(async {
        // metrics
        let start = Instant::now();
        let mut read_batch_time = 0;
        let mut accum_batch_time = 0;
        let mut batch_count: i32 = 0;
        let mut row_count = 0;

        // hash map of grouping values to accumulators
        let mut map: HashMap<Vec<GroupByScalar>, AccumulatorSet> = HashMap::new();

        // create vector large enough to hold the grouping key that can be re-used per row to
        // avoid the cost of creating a new vector each time
        let mut key = Vec::with_capacity(group_expr.len());
        for _ in 0..group_expr.len() {
            key.push(GroupByScalar::UInt32(0));
        }

        // iterate over all the input batches .. note that in partial mode it would be valid
        // to emit batches periodically and reset the accumulator map to reduce memory pressure
        loop {
            let read_batch_start = Instant::now();
            let maybe_batch = input.next().await?;
            read_batch_time += read_batch_start.elapsed().as_millis();

            match maybe_batch {
                Some(batch) => {
                    batch_count += 1;
                    row_count += batch.num_rows();

                    let accum_start = Instant::now();

                    // evaluate the grouping expressions against this batch
                    let group_values = group_expr
                        .iter()
                        .map(|e| e.evaluate(&batch))
                        .collect::<Result<Vec<_>>>()?;

                    // evaluate the input expressions to the aggregate functions
                    let aggr_input_values = aggr_expr
                        .iter()
                        .map(|e| e.evaluate_input(&batch))
                        .collect::<Result<Vec<_>>>()?;

                    // we now need to switch to row-based processing :-(
                    for row in 0..batch.num_rows() {
                        // create grouping key for this row
                        create_key(&group_values, row, &mut key)?;

                        // lookup the accumulators for this grouping key
                        let updated = match map.get_mut(&key) {
                            Some(mut accumulators) => {
                                accumulate(&aggr_input_values, &mut accumulators, row)?;
                                true
                            }
                            None => false,
                        };

                        // create the accumulators for this grouping key if they weren't found
                        if !updated {
                            let mut accumulators: AccumulatorSet = aggr_expr
                                .iter()
                                .map(|expr| expr.create_accumulator(&mode))
                                .collect();

                            accumulate(&aggr_input_values, &mut accumulators, row)?;

                            map.insert(key.clone(), accumulators);
                        }
                    }
                    accum_batch_time += accum_start.elapsed().as_millis();
                }
                None => break,
            }
        }

        // prepare results
        let prepare_final_batch_start = Instant::now();
        let batch = create_batch_from_accum_map(
            &map,
            &group_expr,
            &aggr_expr,
            &input_schema,
            &output_schema,
        )?;
        let create_final_batch_time = prepare_final_batch_start.elapsed().as_millis();

        // send the result batch over the channel
        tx.send(Ok(Some(batch))).map_err(|e| {
            ballista_error(&format!("Error sending hash aggregate result: {:?}", e))
        })?;

        // send EOF marker
        tx.send(Ok(None)).map_err(|e| {
            ballista_error(&format!(
                "Error sending hash aggregate end-of-stream: {:?}",
                e
            ))
        })?;

        println!(
            "HashAggregate processed {} batches and {} rows. \
            Reading: {} ms; Accumulating: {} ms; Create result: {} ms. \
            Total duration {} ms.",
            batch_count,
            row_count,
            read_batch_time,
            accum_batch_time,
            create_final_batch_time,
            start.elapsed().as_millis()
        );

        Ok(())
    })
}

#[inline]
fn accumulate(
    aggr_input_values: &[ColumnarValue],
    accumulators: &mut AccumulatorSet,
    row: usize,
) -> Result<()> {
    for (col, value) in aggr_input_values.iter().enumerate() {
        match &value {
            ColumnarValue::Columnar(array) => match array.data_type() {
                DataType::Int8 => update_accumulators!(
                    array,
                    Int8Array,
                    ScalarValue::Int8,
                    row,
                    col,
                    accumulators
                ),
                DataType::Int16 => update_accumulators!(
                    array,
                    Int16Array,
                    ScalarValue::Int16,
                    row,
                    col,
                    accumulators
                ),
                DataType::Int32 => update_accumulators!(
                    array,
                    Int32Array,
                    ScalarValue::Int32,
                    row,
                    col,
                    accumulators
                ),
                DataType::Int64 => update_accumulators!(
                    array,
                    Int64Array,
                    ScalarValue::Int64,
                    row,
                    col,
                    accumulators
                ),
                DataType::UInt8 => update_accumulators!(
                    array,
                    UInt8Array,
                    ScalarValue::UInt8,
                    row,
                    col,
                    accumulators
                ),
                DataType::UInt16 => update_accumulators!(
                    array,
                    UInt16Array,
                    ScalarValue::UInt16,
                    row,
                    col,
                    accumulators
                ),
                DataType::UInt32 => update_accumulators!(
                    array,
                    UInt32Array,
                    ScalarValue::UInt32,
                    row,
                    col,
                    accumulators
                ),
                DataType::UInt64 => update_accumulators!(
                    array,
                    UInt64Array,
                    ScalarValue::UInt64,
                    row,
                    col,
                    accumulators
                ),
                DataType::Float32 => update_accumulators!(
                    array,
                    Float32Array,
                    ScalarValue::Float32,
                    row,
                    col,
                    accumulators
                ),
                DataType::Float64 => update_accumulators!(
                    array,
                    Float64Array,
                    ScalarValue::Float64,
                    row,
                    col,
                    accumulators
                ),
                _other => {
                    unimplemented!()
                    // return Err(BallistaError::General(format!(
                    //     "Unsupported data type {:?} for result of aggregate expression",
                    //     other
                    // )));
                }
            },
            _ => unimplemented!(),
        };
    }
    Ok(())
}

impl HashAggregateIter {
    fn new(
        mode: &AggregateMode,
        input: ColumnarBatchStream,
        group_expr: Vec<Arc<dyn Expression>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input_schema: Arc<Schema>,
        output_schema: Arc<Schema>,
    ) -> Self {
        let (tx, rx): (Sender<MaybeColumnarBatch>, Receiver<MaybeColumnarBatch>) = unbounded();

        let move_input_schema = input_schema.clone();
        let move_output_schema = output_schema.clone();
        let mode = mode.clone();
        let _ = std::thread::spawn(move || {
            run(tx, &mode, input, group_expr, aggr_expr, &move_input_schema, &move_output_schema).unwrap();
        });

        Self {
            schema: output_schema,
            rx,
        }
    }
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
                    let array = cast_array!(col, UInt8Array)?;
                    vec[i] = GroupByScalar::UInt8(array.value(row))
                }
                DataType::UInt16 => {
                    let array = cast_array!(col, UInt16Array)?;
                    vec[i] = GroupByScalar::UInt16(array.value(row))
                }
                DataType::UInt32 => {
                    let array = cast_array!(col, UInt32Array)?;
                    vec[i] = GroupByScalar::UInt32(array.value(row))
                }
                DataType::UInt64 => {
                    let array = cast_array!(col, UInt64Array)?;
                    vec[i] = GroupByScalar::UInt64(array.value(row))
                }
                DataType::Int8 => {
                    let array = cast_array!(col, Int8Array)?;
                    vec[i] = GroupByScalar::Int8(array.value(row))
                }
                DataType::Int16 => {
                    let array = cast_array!(col, Int16Array)?;
                    vec[i] = GroupByScalar::Int16(array.value(row))
                }
                DataType::Int32 => {
                    let array = cast_array!(col, Int32Array)?;
                    vec[i] = GroupByScalar::Int32(array.value(row))
                }
                DataType::Int64 => {
                    let array = cast_array!(col, Int64Array)?;
                    vec[i] = GroupByScalar::Int64(array.value(row))
                }
                DataType::Utf8 => {
                    let array = cast_array!(col, StringArray)?;
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
    map: &HashMap<Vec<GroupByScalar>, AccumulatorSet>,
    group_expr: &[Arc<dyn Expression>],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    input_schema: &Schema,
    output_schema: &Schema,
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
                        GroupByScalar::Utf8(s) => builder.append_value(&s)?,
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

    Ok(ColumnarBatch::from_arrow(&RecordBatch::try_new(Arc::new(output_schema.clone()), arrays)?))
}

#[async_trait]
impl ColumnarBatchIter for HashAggregateIter {
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
