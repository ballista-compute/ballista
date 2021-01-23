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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::{convert::TryInto, unimplemented};

use crate::error::BallistaError;
use crate::serde::{proto_error, protobuf};

use arrow::datatypes::{DataType, DateUnit, Field, Schema};
use datafusion::logical_plan::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::scalar::ScalarValue;
use protobuf::logical_expr_node::ExprType;

// use uuid::Uuid;

macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

impl TryInto<LogicalPlan> for &protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        if let Some(projection) = &self.projection {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .project(
                    projection
                        .expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(selection) = &self.selection {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .filter(
                    selection
                        .expr
                        .as_ref()
                        .expect("expression required")
                        .try_into()?,
                )?
                .build()
                .map_err(|e| e.into())
        } else if let Some(aggregate) = &self.aggregate {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            let group_expr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr.try_into())
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_expr = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.try_into())
                .collect::<Result<Vec<_>, _>>()?;
            LogicalPlanBuilder::from(&input)
                .aggregate(group_expr, aggr_expr)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(scan) = &self.csv_scan {
            let schema: Schema = convert_required!(scan.schema)?;
            let options = CsvReadOptions::new()
                .schema(&schema)
                .delimiter(scan.delimiter.as_bytes()[0])
                .file_extension(&scan.file_extension)
                .has_header(scan.has_header);
            let mut projection = None;
            if let Some(column_names) = &scan.projection {
                let column_indices = column_names
                    .columns
                    .iter()
                    .map(|name| schema.index_of(name))
                    .collect::<Result<Vec<usize>, _>>()?;
                projection = Some(column_indices);
            }

            LogicalPlanBuilder::scan_csv(&scan.path, options, projection)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(scan) = &self.parquet_scan {
            LogicalPlanBuilder::scan_parquet(&scan.path, None, 24)? //TODO projection, concurrency
                .build()
                .map_err(|e| e.into())
        } else if let Some(sort) = &self.sort {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            let sort_expr: Vec<Expr> = sort
                .expr
                .iter()
                .map(|expr| expr.try_into())
                .collect::<Result<Vec<Expr>, _>>()?;
            LogicalPlanBuilder::from(&input)
                .sort(sort_expr)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(repartition) = &self.repartition {
            use datafusion::logical_plan::Partitioning;
            let input: LogicalPlan = convert_box_required!(self.input)?;
            use protobuf::repartition_node::PartitionMethod;
            let pb_partition_method = repartition.partition_method.clone()
                                                                            .ok_or_else(
                                                                                || BallistaError::General(String::from("Protobuf deserialization error, RepartitionNode was missing required field 'partition_method'"))
                                                                            )?;

            let partitioning_scheme = match pb_partition_method {
                PartitionMethod::Hash(protobuf::HashRepartition {
                    hash_expr: pb_hash_expr,
                    batch_size,
                }) => Partitioning::Hash(
                    pb_hash_expr
                        .iter()
                        .map(|pb_expr| pb_expr.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                    batch_size as usize,
                ),
                PartitionMethod::RoundRobin(batch_size) => {
                    Partitioning::RoundRobinBatch(batch_size as usize)
                }
            };

            LogicalPlanBuilder::from(&input)
                .repartition(partitioning_scheme)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(empty_relation) = &self.empty_relation {
            LogicalPlanBuilder::empty(empty_relation.produce_one_row)
                .build()
                .map_err(|e| e.into())
        } else if let Some(create_extern_table) = &self.create_external_table {
            let pb_schema = (create_extern_table.schema.clone())
                                            .ok_or_else(
                                                || BallistaError::General(String::from("Protobuf deserialization error, CreateExternalTableNode was missing required field schema."))
                                            )?;

            let pb_file_type: protobuf::FileType = create_extern_table.file_type.try_into()?;

            Ok(LogicalPlan::CreateExternalTable {
                schema: pb_schema.try_into()?,
                name: create_extern_table.name.clone(),
                location: create_extern_table.location.clone(),
                file_type: pb_file_type.into(),
                has_header: create_extern_table.has_header,
            })
        } else if let Some(explain) = &self.explain {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .explain(explain.verbose)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(limit) = &self.limit {
            let input: LogicalPlan = convert_box_required!(self.input)?;
            LogicalPlanBuilder::from(&input)
                .limit(limit.limit as usize)?
                .build()
                .map_err(|e| e.into())
        } else if let Some(join) = &self.join {
            assert!(self.input.is_none(), "Received a LogicalPlanNode that contains a JoinNode and also a nested LogicalPlanNode");
            let left_keys: Vec<&str> = join.left_join_column.iter().map(|i| i.as_str()).collect();
            let right_keys: Vec<&str> = join.right_join_column.iter().map(|i| i.as_str()).collect();
            let join_type = protobuf::JoinType::from_i32(join.join_type).ok_or_else(|| {
                proto_error(format!(
                    "Received a JoinNode message with unknwown JoinType {}",
                    join.join_type
                ))
            })?;
            let join_type = match join_type {
                protobuf::JoinType::Inner => JoinType::Inner,
                protobuf::JoinType::Left => JoinType::Left,
                protobuf::JoinType::Right => JoinType::Right,
            };
            LogicalPlanBuilder::from(&convert_box_required!(join.left)?)
                .join(
                    &convert_box_required!(join.right)?,
                    join_type,
                    &left_keys,
                    &right_keys,
                )?
                .build()
                .map_err(|e| e.into())
        } else {
            Err(proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{:?}'",
                self
            )))
        }
    }
}

impl TryInto<datafusion::logical_plan::DFSchema> for protobuf::Schema {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::logical_plan::DFSchema, Self::Error> {
        let schema: Schema = (&self).try_into()?;
        schema.try_into().map_err(BallistaError::DataFusionError)
    }
}

impl TryInto<datafusion::logical_plan::DFSchemaRef> for protobuf::Schema {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::logical_plan::DFSchemaRef, Self::Error> {
        use datafusion::logical_plan::ToDFSchema;
        let schema: Schema = (&self).try_into()?;
        schema
            .to_dfschema_ref()
            .map_err(BallistaError::DataFusionError)
    }
}

impl TryInto<Expr> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        let expr_type = self
            .expr_type
            .as_ref()
            .ok_or_else(|| proto_error("Unexpected empty logical expression"))?;
        match expr_type {
            ExprType::BinaryExpr(binary_expr) => Ok(Expr::BinaryExpr {
                left: Box::new(parse_required_expr(&binary_expr.l)?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(parse_required_expr(&binary_expr.r)?),
            }),
            ExprType::ColumnName(column_name) => Ok(Expr::Column(column_name.to_owned())),
            ExprType::LiteralString(literal_string) => Ok(Expr::Literal(ScalarValue::Utf8(Some(
                literal_string.to_owned(),
            )))),
            ExprType::LiteralF32(value) => Ok(Expr::Literal(ScalarValue::Float32(Some(*value)))),
            ExprType::LiteralF64(value) => Ok(Expr::Literal(ScalarValue::Float64(Some(*value)))),
            ExprType::LiteralInt8(value) => {
                Ok(Expr::Literal(ScalarValue::Int8(Some(*value as i8))))
            }
            ExprType::LiteralInt16(value) => {
                Ok(Expr::Literal(ScalarValue::Int16(Some(*value as i16))))
            }
            ExprType::LiteralInt32(value) => Ok(Expr::Literal(ScalarValue::Int32(Some(*value)))),
            ExprType::LiteralInt64(value) => Ok(Expr::Literal(ScalarValue::Int64(Some(*value)))),
            ExprType::LiteralUint8(value) => {
                Ok(Expr::Literal(ScalarValue::UInt8(Some(*value as u8))))
            }
            ExprType::LiteralUint16(value) => {
                Ok(Expr::Literal(ScalarValue::UInt16(Some(*value as u16))))
            }
            ExprType::LiteralUint32(value) => Ok(Expr::Literal(ScalarValue::UInt32(Some(*value)))),
            ExprType::LiteralUint64(value) => Ok(Expr::Literal(ScalarValue::UInt64(Some(*value)))),
            ExprType::LiteralNull(arrow_type) => {
                let arrow_type = protobuf::ArrowType::from_i32(*arrow_type).ok_or_else(|| {
                    proto_error(format!(
                        "Received a LiteralNull message with unknwown type {}",
                        arrow_type
                    ))
                })?;
                match arrow_type {
                    protobuf::ArrowType::Int8 => Ok(Expr::Literal(ScalarValue::Int8(None))),
                    protobuf::ArrowType::Int16 => Ok(Expr::Literal(ScalarValue::Int16(None))),
                    protobuf::ArrowType::Int32 => Ok(Expr::Literal(ScalarValue::Int32(None))),
                    protobuf::ArrowType::Int64 => Ok(Expr::Literal(ScalarValue::Int64(None))),
                    protobuf::ArrowType::Uint8 => Ok(Expr::Literal(ScalarValue::UInt8(None))),
                    protobuf::ArrowType::Uint16 => Ok(Expr::Literal(ScalarValue::UInt16(None))),
                    protobuf::ArrowType::Uint32 => Ok(Expr::Literal(ScalarValue::UInt32(None))),
                    protobuf::ArrowType::Uint64 => Ok(Expr::Literal(ScalarValue::UInt64(None))),
                    protobuf::ArrowType::Utf8 => Ok(Expr::Literal(ScalarValue::Utf8(None))),
                    protobuf::ArrowType::Float => Ok(Expr::Literal(ScalarValue::Float32(None))),
                    protobuf::ArrowType::Double => Ok(Expr::Literal(ScalarValue::Float64(None))),
                    protobuf::ArrowType::None => Err(proto_error("Received untyped null value")),
                    _ => unimplemented!(),
                }
            }
            ExprType::AggregateExpr(expr) => {
                let aggr_function = protobuf::AggregateFunction::from_i32(expr.aggr_function)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown aggregate function: {}",
                            expr.aggr_function
                        ))
                    })?;
                let fun = match aggr_function {
                    protobuf::AggregateFunction::Min => AggregateFunction::Min,
                    protobuf::AggregateFunction::Max => AggregateFunction::Max,
                    protobuf::AggregateFunction::Sum => AggregateFunction::Sum,
                    protobuf::AggregateFunction::Avg => AggregateFunction::Avg,
                    protobuf::AggregateFunction::Count => AggregateFunction::Count,
                };

                Ok(Expr::AggregateFunction {
                    fun,
                    args: vec![parse_required_expr(&expr.expr)?],
                    distinct: false, //TODO
                })
            }
            ExprType::Alias(alias) => Ok(Expr::Alias(
                Box::new(parse_required_expr(&alias.expr)?),
                alias.alias.clone(),
            )),
            ExprType::IsNullExpr(is_null) => {
                Ok(Expr::IsNull(Box::new(parse_required_expr(&is_null.expr)?)))
            }
            ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
                parse_required_expr(&is_not_null.expr)?,
            ))),
            ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(&not.expr)?))),
            ExprType::Between(between) => Ok(Expr::Between {
                expr: Box::new(parse_required_expr(&between.expr)?),
                negated: between.negated,
                low: Box::new(parse_required_expr(&between.low)?),
                high: Box::new(parse_required_expr(&between.high)?),
            }),
            ExprType::Case(case) => {
                let when_then_expr = case
                    .when_then_expr
                    .iter()
                    .map(|e| {
                        Ok((
                            Box::new(match &e.when_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                            Box::new(match &e.then_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                        ))
                    })
                    .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, BallistaError>>()?;
                Ok(Expr::Case {
                    expr: parse_optional_expr(&case.expr)?.map(Box::new),
                    when_then_expr,
                    else_expr: parse_optional_expr(&case.else_expr)?.map(Box::new),
                })
            }
            ExprType::Cast(cast) => Ok(Expr::Cast {
                expr: Box::new(parse_required_expr(&cast.expr)?),
                data_type: from_proto_arrow_type(cast.arrow_type)?,
            }),
            ExprType::Sort(sort) => Ok(Expr::Sort {
                expr: Box::new(parse_required_expr(&sort.expr)?),
                asc: sort.asc,
                nulls_first: sort.nulls_first,
            }),
            ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(parse_required_expr(
                &negative.expr,
            )?))),
            ExprType::InList(in_list) => Ok(Expr::InList {
                expr: Box::new(parse_required_expr(&in_list.expr)?),
                list: in_list
                    .list
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                negated: in_list.negated,
            }),
            ExprType::Wildcard(_) => Ok(Expr::Wildcard),
        }
    }
}

fn from_proto_binary_op(op: &str) -> Result<Operator, BallistaError> {
    match op {
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

fn from_proto_arrow_type(dt: i32) -> Result<DataType, BallistaError> {
    match dt {
        dt if dt == protobuf::ArrowType::Bool as i32 => Ok(DataType::Boolean),
        dt if dt == protobuf::ArrowType::Uint8 as i32 => Ok(DataType::UInt8),
        dt if dt == protobuf::ArrowType::Int8 as i32 => Ok(DataType::Int8),
        dt if dt == protobuf::ArrowType::Uint16 as i32 => Ok(DataType::UInt16),
        dt if dt == protobuf::ArrowType::Int16 as i32 => Ok(DataType::Int16),
        dt if dt == protobuf::ArrowType::Uint32 as i32 => Ok(DataType::UInt32),
        dt if dt == protobuf::ArrowType::Int32 as i32 => Ok(DataType::Int32),
        dt if dt == protobuf::ArrowType::Uint64 as i32 => Ok(DataType::UInt64),
        dt if dt == protobuf::ArrowType::Int64 as i32 => Ok(DataType::Int64),
        dt if dt == protobuf::ArrowType::HalfFloat as i32 => Ok(DataType::Float16),
        dt if dt == protobuf::ArrowType::Float as i32 => Ok(DataType::Float32),
        dt if dt == protobuf::ArrowType::Double as i32 => Ok(DataType::Float64),
        dt if dt == protobuf::ArrowType::Utf8 as i32 => Ok(DataType::Utf8),
        dt if dt == protobuf::ArrowType::Date32Day as i32 => Ok(DataType::Date32(DateUnit::Day)),
        dt if dt == protobuf::ArrowType::Date32Millisecond as i32 => {
            Ok(DataType::Date32(DateUnit::Millisecond))
        }
        dt if dt == protobuf::ArrowType::Binary as i32 => Ok(DataType::Binary),
        other => Err(BallistaError::General(format!(
            "Unsupported data type {:?}",
            other
        ))),
    }
}

// impl TryInto<ExecutionTask> for &protobuf::Task {
//     type Error = BallistaError;
//
//     fn try_into(self) -> Result<ExecutionTask, Self::Error> {
//         let mut shuffle_locations: HashMap<ShuffleId, ExecutorMeta> = HashMap::new();
//         for loc in &self.shuffle_loc {
//             let shuffle_id = ShuffleId::new(
//                 Uuid::parse_str(&loc.job_uuid).expect("error parsing uuid in from_proto"),
//                 loc.stage_id as usize,
//                 loc.partition_id as usize,
//             );
//
//             let exec = ExecutorMeta {
//                 id: loc.executor_id.to_owned(),
//                 host: loc.executor_host.to_owned(),
//                 port: loc.executor_port as usize,
//             };
//
//             shuffle_locations.insert(shuffle_id, exec);
//         }
//
//         Ok(ExecutionTask::new(
//             Uuid::parse_str(&self.job_uuid).expect("error parsing uuid in from_proto"),
//             self.stage_id as usize,
//             self.partition_id as usize,
//             convert_required!(self.plan)?,
//             shuffle_locations,
//         ))
//     }
// }
//
// impl TryInto<ShuffleLocation> for &protobuf::ShuffleLocation {
//     type Error = BallistaError;
//
//     fn try_into(self) -> Result<ShuffleLocation, Self::Error> {
//         Ok(ShuffleLocation {}) //TODO why empty?
//     }
// }
//
// impl TryInto<ShuffleId> for &protobuf::ShuffleId {
//     type Error = BallistaError;
//
//     fn try_into(self) -> Result<ShuffleId, Self::Error> {
//         Ok(ShuffleId::new(
//             Uuid::parse_str(&self.job_uuid).expect("error parsing uuid in from_proto"),
//             self.stage_id as usize,
//             self.partition_id as usize,
//         ))
//     }
// }

impl TryInto<Schema> for &protobuf::Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<Schema, Self::Error> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let dt: Result<DataType, _> = from_proto_arrow_type(c.arrow_type);
                dt.map(|dt| Field::new(&c.name, dt, c.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

use std::convert::TryFrom;
impl TryFrom<i32> for protobuf::FileType {
    type Error = BallistaError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use protobuf::FileType;
        match value {
            _x if _x == FileType::NdJson as i32 => Ok(FileType::NdJson),
            _x if _x == FileType::Parquet as i32 => Ok(FileType::Parquet),
            _x if _x == FileType::Csv as i32 => Ok(FileType::Csv),
            invalid => Err(BallistaError::General(format!(
                "Attempted to convert invalid i32 to protobuf::Filetype: {}",
                invalid
            ))),
        }
    }
}

impl Into<datafusion::sql::parser::FileType> for protobuf::FileType {
    fn into(self) -> datafusion::sql::parser::FileType {
        use datafusion::sql::parser::FileType;
        match self {
            protobuf::FileType::NdJson => FileType::NdJson,
            protobuf::FileType::Parquet => FileType::Parquet,
            protobuf::FileType::Csv => FileType::CSV,
        }
    }
}

// impl TryInto<PhysicalPlan> for &protobuf::PhysicalPlanNode {
//     type Error = BallistaError;
//
//     fn try_into(self) -> Result<PhysicalPlan, Self::Error> {
//         if let Some(selection) = &self.selection {
//             let input: PhysicalPlan = convert_box_required!(self.input)?;
//             match selection.expr {
//                 Some(ref protobuf_expr) => {
//                     let expr: Expr = protobuf_expr.try_into()?;
//                     Ok(PhysicalPlan::Filter(Arc::new(FilterExec::new(
//                         &input, &expr,
//                     ))))
//                 }
//                 _ => Err(proto_error("from_proto: Selection expr missing")),
//             }
//         } else if let Some(projection) = &self.projection {
//             let input: PhysicalPlan = convert_box_required!(self.input)?;
//             let exprs = projection
//                 .expr
//                 .iter()
//                 .map(|expr| expr.try_into())
//                 .collect::<Result<Vec<_>, _>>()?;
//             Ok(PhysicalPlan::Projection(Arc::new(ProjectionExec::try_new(
//                 &exprs,
//                 Arc::new(input),
//             )?)))
//         } else if let Some(aggregate) = &self.hash_aggregate {
//             let input: PhysicalPlan = convert_box_required!(self.input)?;
//             let mode = match aggregate.mode {
//                 mode if mode == protobuf::AggregateMode::Partial as i32 => {
//                     Ok(AggregateMode::Partial)
//                 }
//                 mode if mode == protobuf::AggregateMode::Final as i32 => Ok(AggregateMode::Final),
//                 mode if mode == protobuf::AggregateMode::Complete as i32 => {
//                     Ok(AggregateMode::Complete)
//                 }
//                 other => Err(proto_error(&format!(
//                     "Unsupported aggregate mode '{}' for hash aggregate",
//                     other
//                 ))),
//             }?;
//             let group_expr = aggregate
//                 .group_expr
//                 .iter()
//                 .map(|expr| expr.try_into())
//                 .collect::<Result<Vec<_>, _>>()?;
//             let aggr_expr = aggregate
//                 .aggr_expr
//                 .iter()
//                 .map(|expr| expr.try_into())
//                 .collect::<Result<Vec<_>, _>>()?;
//             Ok(PhysicalPlan::HashAggregate(Arc::new(
//                 HashAggregateExec::try_new(mode, group_expr, aggr_expr, Arc::new(input))?,
//             )))
//         } else if let Some(scan) = &self.scan {
//             match scan.file_format.as_str() {
//                 "csv" => {
//                     let schema: Schema = convert_required!(scan.schema)?;
//                     let options = CsvReadOptions::new()
//                         .schema(&schema)
//                         .has_header(scan.has_header);
//                     let projection = scan.projection.iter().map(|n| *n as usize).collect();
//
//                     Ok(PhysicalPlan::CsvScan(Arc::new(CsvScanExec::try_new(
//                         &scan.path,
//                         scan.filename.clone(),
//                         options,
//                         Some(projection),
//                         scan.batch_size as usize,
//                     )?)))
//                 }
//                 "parquet" => {
//                     let schema: Schema = convert_required!(scan.schema)?;
//                     Ok(PhysicalPlan::ParquetScan(Arc::new(
//                         ParquetScanExec::try_new(
//                             &scan.path,
//                             scan.filename.clone(),
//                             Some(scan.projection.iter().map(|n| *n as usize).collect()),
//                             scan.batch_size as usize,
//                             Some(schema),
//                         )?,
//                     )))
//                 }
//                 other => Err(proto_error(&format!(
//                     "Unsupported file format '{}' for file scan",
//                     other
//                 ))),
//             }
//         } else if let Some(shuffle_reader) = &self.shuffle_reader {
//             let mut shuffle_ids = vec![];
//             for s in &shuffle_reader.shuffle_id {
//                 shuffle_ids.push(s.try_into()?);
//             }
//             Ok(PhysicalPlan::ShuffleReader(Arc::new(
//                 ShuffleReaderExec::new(
//                     Arc::new(convert_required!(shuffle_reader.schema)?),
//                     shuffle_ids,
//                 ),
//             )))
//         } else {
//             Err(proto_error(&format!(
//                 "Unsupported physical plan '{:?}'",
//                 self
//             )))
//         }
//     }
// }

fn parse_required_expr(p: &Option<Box<protobuf::LogicalExprNode>>) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into(),
        None => Err(proto_error("Missing required expression")),
    }
}

fn parse_optional_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Option<Expr>, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into().map(Some),
        None => Ok(None),
    }
}
