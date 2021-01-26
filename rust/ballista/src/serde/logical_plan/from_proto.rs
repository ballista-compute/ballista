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

use arrow::datatypes::{ DataType, Field, Schema};
use datafusion::logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::scalar::ScalarValue;
use prost_types::field;
use protobuf::{BasicDatafusionScalarType, ScalarListType, ScalarListValue, ScalarType, arrow_type, scalar_type::{self, Datatype}};

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


impl TryInto<arrow::datatypes::Field> for &protobuf::Field{
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::Field, Self::Error> {
        let pb_datatype= self.arrow_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: Field message missing required field 'arrow_type'"))?;
        
        Ok(arrow::datatypes::Field::new(
             &self.name[..],
             pb_datatype.as_ref().try_into()?,
             self.nullable,
        ))
    }
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


impl TryInto<arrow::datatypes::DataType> for &protobuf::scalar_type::Datatype{
    type Error = BallistaError;
    fn try_into(self)->Result<arrow::datatypes::DataType, Self::Error>{
        Ok(match self{
            Datatype::Scalar(scalar_type) => {
                let pb_scalar_enum = protobuf::BasicDatafusionScalarType::from_i32(*scalar_type)
                                                                        .ok_or_else(|| proto_error("Protobuf deserialization error, scalar_type::Datatype missing was provided invalid enum variant"))?;
                pb_scalar_enum.into()                                                 
            }
            Datatype::List(protobuf::ScalarListType{depth, deepest_type, field_names }) => {

                if *depth != (field_names.len() -1) as u64{
                    return Err(proto_error(format!("Protobuf deserialization error, found {} field names should be {}", field_names.len(), depth + 1)));
                }
                let pb_scalar_type = protobuf::BasicDatafusionScalarType::from_i32(*deepest_type)
                                                                    .ok_or_else(||proto_error("Protobuf deserialization error: invalid i32 for scalar enum"))?;
                let mut field_name_idx = field_names.len()-1;
                let mut scalar_type = arrow::datatypes::DataType::List(Box::new(
                    Field::new(&field_names[field_names.len() - 1][..], pb_scalar_type.into(), true)
                ));
                for _ in 0..*depth{
                    field_name_idx = field_name_idx -1;
                    let new_datatype = arrow::datatypes::DataType::List(Box::new(Field::new(&field_names[field_name_idx][..], scalar_type, true)));
                    scalar_type = new_datatype;
                    
                }
                scalar_type
            },
        })
    }
}


impl TryInto<arrow::datatypes::DataType> for &protobuf::arrow_type::ArrowTypeEnum{
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        use arrow::datatypes::DataType;
        Ok(match self{
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(date_unit) => DataType::Date32(protobuf::DateUnit::from_i32_to_arrow(*date_unit)?),
            arrow_type::ArrowTypeEnum::Date64(date_unit) => DataType::Date64(protobuf::DateUnit::from_i32_to_arrow(*date_unit)?),
            arrow_type::ArrowTypeEnum::Duration(time_unit) => DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?),
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp{time_unit, timezone}) => DataType::Timestamp(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?, match timezone.len(){
                0 => None,
                _ => Some(timezone.to_owned()),
            }),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?),
            arrow_type::ArrowTypeEnum::Time64(time_unit) => DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?),
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => DataType::Interval(protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?),
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal{whole, fractional}) => DataType::Decimal(*whole as usize, *fractional as usize),
            arrow_type::ArrowTypeEnum::List(list) =>{
                let list_type: &protobuf::Field = list.as_ref().field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?.as_ref();
                DataType::List(Box::new(list_type.try_into()?))
            },
            arrow_type::ArrowTypeEnum::LargeList(list) =>{
                let list_type: &protobuf::Field = list.as_ref().field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?.as_ref();
                DataType::LargeList(Box::new(list_type.try_into()?))
            },
            arrow_type::ArrowTypeEnum::FixedSizeList(list) =>{
                let list_type: &protobuf::Field = list.as_ref().field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?.as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type.try_into()?), list_size)
            },
            arrow_type::ArrowTypeEnum::Struct(strct) => {
                DataType::Struct(strct.sub_field_types.iter()
                                        .map(|field| field.try_into())
                                        .collect::<Result<Vec<_>, _>>()?)
            },
            arrow_type::ArrowTypeEnum::Union(union) => {
                DataType::Union(union.union_types.iter()
                                        .map(|field| field.try_into())
                                        .collect::<Result<Vec<_>, _>>()?)
            },
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let pb_key_datatype = dict.as_ref().key.as_ref().ok_or_else(||proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let pb_value_datatype = dict.as_ref().value.as_ref().ok_or_else(||proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let key_datatype: DataType = pb_key_datatype.as_ref().try_into()?;
                let value_datatype: DataType = pb_value_datatype.as_ref().try_into()?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}


impl Into<arrow::datatypes::DataType> for protobuf::BasicDatafusionScalarType{
    fn into(self)->arrow::datatypes::DataType{
        use arrow::datatypes::DataType;
        match self{
            protobuf::BasicDatafusionScalarType::Bool => DataType::Boolean,
            protobuf::BasicDatafusionScalarType::Uint8 => DataType::UInt8,
            protobuf::BasicDatafusionScalarType::Int8 => DataType::Int8,
            protobuf::BasicDatafusionScalarType::Uint16 => DataType::UInt16,
            protobuf::BasicDatafusionScalarType::Int16 => DataType::Int16,
            protobuf::BasicDatafusionScalarType::Uint32 => DataType::UInt32,
            protobuf::BasicDatafusionScalarType::Int32 => DataType::Int32,
            protobuf::BasicDatafusionScalarType::Uint64 => DataType::UInt64,
            protobuf::BasicDatafusionScalarType::Int64 => DataType::Int64,
            protobuf::BasicDatafusionScalarType::Float32 => DataType::Float32,
            protobuf::BasicDatafusionScalarType::Float64 => DataType::Float64,
            protobuf::BasicDatafusionScalarType::Utf8 => DataType::Utf8,
            protobuf::BasicDatafusionScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::BasicDatafusionScalarType::Date32 => DataType::Date32(arrow::datatypes::DateUnit::Day),
            protobuf::BasicDatafusionScalarType::TimeMicrosecond => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            protobuf::BasicDatafusionScalarType::TimeNanosecond => DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
            protobuf::BasicDatafusionScalarType::Null => DataType::Null,
        }
    }
}

//Does not typecheck lists
fn typechecked_scalar_value_conversion(tested_type: &protobuf::scalar_value::Value, required_type: BasicDatafusionScalarType)->Result<datafusion::scalar::ScalarValue, BallistaError>{
    use protobuf::scalar_value::Value;
    Ok(match (tested_type, &required_type){
        (Value::BoolValue(v), BasicDatafusionScalarType::Bool) => ScalarValue::Boolean(Some(*v)),
        (Value::Int8Value(v), BasicDatafusionScalarType::Int8)=> ScalarValue::Int8(Some(*v as i8)),
        (Value::Int16Value(v), BasicDatafusionScalarType::Int16)=> ScalarValue::Int16(Some(*v as i16)),
        (Value::Int32Value(v), BasicDatafusionScalarType::Int32)=> ScalarValue::Int32(Some(*v)),
        (Value::Int64Value(v), BasicDatafusionScalarType::Int64)=> ScalarValue::Int64(Some(*v)),
        (Value::Uint8Value(v), BasicDatafusionScalarType:: Uint8)=> ScalarValue::UInt8(Some(*v as u8)),
        (Value::Uint16Value(v), BasicDatafusionScalarType::Uint16)=> ScalarValue::UInt16(Some(*v as u16)),
        (Value::Uint32Value(v), BasicDatafusionScalarType::Uint32)=> ScalarValue::UInt32(Some(*v)),
        (Value::Uint64Value(v), BasicDatafusionScalarType::Uint64)=> ScalarValue::UInt64(Some(*v)),
        (Value::Float32Value(v), BasicDatafusionScalarType::Float32) => ScalarValue::Float32(Some(*v)),
        (Value::Float64Value(v), BasicDatafusionScalarType::Float64) => ScalarValue::Float64(Some(*v)),
        (Value::Date32Value(v), BasicDatafusionScalarType::Date32)=> ScalarValue::Date32(Some(*v)),
        (Value::TimeMicrosecondValue(v), BasicDatafusionScalarType::TimeMicrosecond)=>ScalarValue::TimeMicrosecond(Some(*v)),
        (Value::TimeNanosecondValue(v), BasicDatafusionScalarType::TimeMicrosecond)=>ScalarValue::TimeNanosecond(Some(*v)),
        (Value::Utf8Value(v), BasicDatafusionScalarType::Utf8) => ScalarValue::Utf8(Some(v.to_owned())),
        (Value::LargeUtf8Value(v), BasicDatafusionScalarType::LargeUtf8) => ScalarValue::LargeUtf8(Some(v.to_owned())),
        
        (Value::NullValue(i32_enum), required_scalar_type)=>{
            if *i32_enum == *required_scalar_type as i32{
                let pb_scalar_type = BasicDatafusionScalarType::from_i32(*i32_enum).unwrap();
                let scalar_value: ScalarValue = match pb_scalar_type{
                    BasicDatafusionScalarType::Bool => ScalarValue::Boolean(None),
                    BasicDatafusionScalarType::Uint8 => ScalarValue::UInt8(None),
                    BasicDatafusionScalarType::Int8 => ScalarValue::Int8(None),
                    BasicDatafusionScalarType::Uint16 => ScalarValue::UInt16(None),
                    BasicDatafusionScalarType::Int16 => ScalarValue::Int16(None),
                    BasicDatafusionScalarType::Uint32 => ScalarValue::UInt32(None),
                    BasicDatafusionScalarType::Int32 => ScalarValue::Int32(None),
                    BasicDatafusionScalarType::Uint64 =>ScalarValue::UInt64(None),
                    BasicDatafusionScalarType::Int64 => ScalarValue::Int64(None),
                    BasicDatafusionScalarType::Float32 => ScalarValue::Float32(None),
                    BasicDatafusionScalarType::Float64 => ScalarValue::Float64(None),
                    BasicDatafusionScalarType::Utf8 => ScalarValue::Utf8(None),
                    BasicDatafusionScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                    BasicDatafusionScalarType::Date32 => ScalarValue::Date32(None),
                    BasicDatafusionScalarType::TimeMicrosecond => ScalarValue::TimeMicrosecond(None),
                    BasicDatafusionScalarType::TimeNanosecond => ScalarValue::TimeNanosecond(None),
                    BasicDatafusionScalarType::Null=> return Err(proto_error("Untyped scalar null is not a valid scalar value")),
                };
                scalar_value
            }else{
                return Err(proto_error("Could not convert to the proper type"));
            }
        },
        _=> return Err(proto_error("Could not convert to the proper type")),
    })
}


impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::scalar_value::Value{
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        let scalar = match self{
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => ScalarValue::LargeUtf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => ScalarValue::TimeMicrosecond(Some(*v)),
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => ScalarValue::TimeNanosecond(Some(*v)),
            protobuf::scalar_value::Value::ListValue(v) => v.try_into()?,
            protobuf::scalar_value::Value::NullListValue(v) => ScalarValue::List(None, v.try_into()?),
            protobuf::scalar_value::Value::NullValue(null_enum) => BasicDatafusionScalarType::from_i32(*null_enum).ok_or_else(||proto_error("Invalid scalar type"))?.try_into()?,
        };
        Ok(scalar)
    }
}


impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarListValue{
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        let protobuf::ScalarListValue{datatype, values} = self;
        let pb_scalar_type = datatype.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue messsage missing required field 'datatype'"))?;
        let scalar_type = pb_scalar_type.datatype.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue.Datatype messsage missing required field 'datatype'"))?;
        let scalar_values = match scalar_type{
            Datatype::Scalar(scalar_type_i32) => {
                let leaf_scalar_type = protobuf::BasicDatafusionScalarType::from_i32(*scalar_type_i32)
                                                                        .ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
                let typechecked_values:Vec<datafusion::scalar::ScalarValue> = values.iter()
                    .map(|protobuf::ScalarValue{value: opt_value}| { 
                        let value = opt_value.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                        typechecked_scalar_value_conversion(value, leaf_scalar_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                datafusion::scalar::ScalarValue::List(Some(typechecked_values), leaf_scalar_type.into())                                                
            }
            Datatype::List(protobuf::ScalarListType{depth, deepest_type, field_names}) => {
                let leaf_type = BasicDatafusionScalarType::from_i32(*deepest_type).ok_or_else(|| proto_error("Error converting i32 to basic scalar type"))?;
                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = 
                if *depth == 0{
                    values.iter()
                                    .map(|protobuf::ScalarValue{value: opt_value}|{
                                        let value = opt_value.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                                        typechecked_scalar_value_conversion(value, leaf_type)
                                    })
                                    .collect::<Result<Vec<_>,_>>()?
                }else{
                    values.iter()
                    .map(|protobuf::ScalarValue{value: opt_value}|{
                        let value = opt_value.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                        value.try_into()
                    })
                    .collect::<Result<Vec<_>,_>>()?
                };
                let temp_slt = protobuf::ScalarListType{depth: *depth,deepest_type: *deepest_type, field_names: field_names.clone()};
                datafusion::scalar::ScalarValue::List(match typechecked_values.len(){
                    0 => None,
                    _ => Some(typechecked_values)
                }, (&temp_slt).try_into()?)
            }
        };
        Ok(scalar_values)
    }
}

impl TryInto<arrow::datatypes::DataType> for &protobuf::ScalarListType{
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        let protobuf::ScalarListType{depth, deepest_type, field_names} = self;
        let mut name_idx = field_names.len() - 1;
        let mut curr_type = arrow::datatypes::DataType::List(Box::new(
            Field::new(&field_names[name_idx][..],
            BasicDatafusionScalarType::from_i32(*deepest_type).ok_or_else(||proto_error("Could not convert to datafusion scalar type"))?.into(),
            true
        )));
        name_idx -= 1;
        for _ in (0..*depth).rev(){
            let temp_curr_type = arrow::datatypes::DataType::List(Box::new(
                Field::new(&field_names[name_idx][..],
                curr_type,
                true    
            )));
            name_idx -=1;
            curr_type = temp_curr_type;
        }
        Ok(curr_type)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for BasicDatafusionScalarType{
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        Ok(match self{
            protobuf::BasicDatafusionScalarType::Null => Err(proto_error("Untyped null is an invalid scalar value"))?,
            protobuf::BasicDatafusionScalarType::Bool => ScalarValue::Boolean(None),
            protobuf::BasicDatafusionScalarType::Uint8 =>ScalarValue::UInt8(None),
            protobuf::BasicDatafusionScalarType::Int8 =>ScalarValue::Int8(None),
            protobuf::BasicDatafusionScalarType::Uint16 =>ScalarValue::UInt16(None),
            protobuf::BasicDatafusionScalarType::Int16 =>ScalarValue::Int16(None),
            protobuf::BasicDatafusionScalarType::Uint32 =>ScalarValue::UInt32(None),
            protobuf::BasicDatafusionScalarType::Int32 =>ScalarValue::Int32(None),
            protobuf::BasicDatafusionScalarType::Uint64 =>ScalarValue::UInt64(None),
            protobuf::BasicDatafusionScalarType::Int64 =>ScalarValue::Int64(None),
            protobuf::BasicDatafusionScalarType::Float32 =>ScalarValue::Float32(None),
            protobuf::BasicDatafusionScalarType::Float64 =>ScalarValue::Float64(None),
            protobuf::BasicDatafusionScalarType::Utf8 =>ScalarValue::Utf8(None),
            protobuf::BasicDatafusionScalarType::LargeUtf8 =>ScalarValue::LargeUtf8(None),
            protobuf::BasicDatafusionScalarType::Date32 =>ScalarValue::Date32(None),
            protobuf::BasicDatafusionScalarType::TimeMicrosecond =>ScalarValue::TimeMicrosecond(None),
            protobuf::BasicDatafusionScalarType::TimeNanosecond =>ScalarValue::TimeNanosecond(None),
       })
    }
}


impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarValue{
    type Error = BallistaError;
    fn try_into(self)-> Result<datafusion::scalar::ScalarValue, Self::Error>{
        let value = self.value.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
        Ok(match value{
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::LargeUtf8Value(v) => ScalarValue::LargeUtf8(Some(v.to_owned())),
            protobuf::scalar_value::Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            protobuf::scalar_value::Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v  )),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            protobuf::scalar_value::Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            protobuf::scalar_value::Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            protobuf::scalar_value::Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            protobuf::scalar_value::Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            protobuf::scalar_value::Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            protobuf::scalar_value::Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => ScalarValue::TimeMicrosecond(Some(*v)),
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => ScalarValue::TimeNanosecond(Some(*v)),
            protobuf::scalar_value::Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue{values, datatype: opt_scalar_type} = &scalar_list;
                let pb_scalar_type = opt_scalar_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization err: ScalaListValue missing required field 'datatype'"))?;
                let typechecked_values:Vec<ScalarValue> = values.iter()
                    .map(|val| val.try_into())
                    .collect::<Result<Vec<_>,_>>()?;
                let scalar_type: arrow::datatypes::DataType = pb_scalar_type.try_into()?;
                let datatype : arrow::datatypes::DataType = pb_scalar_type.try_into()?; 
                ScalarValue::List(Some(typechecked_values), scalar_type)
            },
            protobuf::scalar_value::Value::NullListValue(v) => {
                let pb_datatype = v.datatype.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: NullListValue message missing required field 'datatyp'"))?;
                ScalarValue::List(None, pb_datatype.try_into()?)
            },
            protobuf::scalar_value::Value::NullValue(v) => {
                let null_type_enum = protobuf::BasicDatafusionScalarType::from_i32(*v).ok_or_else(|| proto_error("Protobuf deserialization error found invalid enum variant for DatafusionScalar"))?;
                null_type_enum.try_into()?
            }
        })
    }
}


impl TryInto<Expr> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        use protobuf::logical_expr_node::ExprType;

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
            ExprType::Literal(literal)=> {
                use datafusion::scalar::ScalarValue;
                let scalar_value: datafusion::scalar::ScalarValue =  literal.try_into()?;
                Ok(Expr::Literal(scalar_value))
            },
            ExprType::AggregateExpr(expr) => {
                let fun = match expr.aggr_function {
                    f if f == protobuf::AggregateFunction::Min as i32 => AggregateFunction::Min,
                    f if f == protobuf::AggregateFunction::Max as i32 => AggregateFunction::Max,
                    f if f == protobuf::AggregateFunction::Sum as i32 => AggregateFunction::Sum,
                    f if f == protobuf::AggregateFunction::Avg as i32 => AggregateFunction::Avg,
                    f if f == protobuf::AggregateFunction::Count as i32 => AggregateFunction::Count,
                    _ => unimplemented!(),
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
            ExprType::Cast(cast) => {
                let expr = Box::new(parse_required_expr(&cast.expr)?);
                let arrow_type: &protobuf::ArrowType = cast.arrow_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: CastNode message missing required field 'arrow_type'"))?;
                let data_type = arrow_type.try_into()?;
                Ok(Expr::Cast{
                    expr: expr,
                    data_type: data_type,
                })
            },
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





impl TryInto<arrow::datatypes::DataType> for &protobuf::ScalarType{
    type Error =  BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        let pb_scalartype = self.datatype.as_ref().ok_or_else(|| proto_error("ScalarType message missing required field 'datatype'"))?;
        let scalar_type: arrow::datatypes::DataType = match pb_scalartype{
            protobuf::scalar_type::Datatype::Scalar(scalar_type_i32)=> {
                let scalar_type = protobuf::BasicDatafusionScalarType::from_i32(*scalar_type_i32)
                                                                    .ok_or_else(||proto_error("Protobuf deserialization error: invalid i32 for scalar enum"))?;
                scalar_type.into()
            },
            protobuf::scalar_type::Datatype::List(protobuf::ScalarListType{depth, deepest_type, field_names})=>{
                if *depth != (field_names.len() -1) as u64{
                    return Err(proto_error(format!("Protobuf deserialization error, found {} field names should be {}", field_names.len(), depth + 1)));
                }
                let pb_scalar_type = protobuf::BasicDatafusionScalarType::from_i32(*deepest_type)
                                                                    .ok_or_else(||proto_error("Protobuf deserialization error: invalid i32 for scalar enum"))?;
                let mut field_name_idx = field_names.len()-1;
                let mut scalar_type = arrow::datatypes::DataType::List(Box::new(
                    Field::new(&field_names[field_name_idx][..], pb_scalar_type.into(), true)
                ));
                
                for _ in 0..*depth{
                    field_name_idx = field_name_idx - 1;
                    let temp_scalar_type = arrow::datatypes::DataType::List(Box::new(
                        Field::new(&field_names[field_name_idx][..], scalar_type, true)
                    ));
                    scalar_type = temp_scalar_type;
                }
                scalar_type
            }
        };
        
        Ok(scalar_type)
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

    fn try_into(self) -> Result<Schema, BallistaError> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let pb_arrow_type_res =  c.arrow_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: Field message was missing required field 'arrow_type'"));
                let pb_arrow_type: &Box<protobuf::ArrowType> = match pb_arrow_type_res{
                    Ok(res)=>res,
                    Err(e) => return Err(e),
                };
                Ok(Field::new(&c.name, pb_arrow_type.as_ref().try_into()?, c.nullable))
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
