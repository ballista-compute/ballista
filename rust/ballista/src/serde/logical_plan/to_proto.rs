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

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::{boxed, convert::TryInto};

use crate::{serde::{empty_logical_plan_node, protobuf, BallistaError}};

use arrow::{datatypes::{DataType, Schema}};
use arrow_type::ArrowTypeEnum;
use datafusion::{datasource::parquet::ParquetTable, logical_plan::exprlist_to_fields};
use datafusion::datasource::CsvFile;
use datafusion::logical_plan::{Expr, JoinType, LogicalPlan};
use datafusion::physical_plan::aggregates::AggregateFunction;
use protobuf::{PrimitiveScalarType, DateUnit, Field, ScalarListValue, ScalarType, arrow_type, logical_expr_node::ExprType, scalar_type};

use super::super::proto_error;

impl protobuf::IntervalUnit{
    pub fn from_arrow_interval_unit(interval_unit: &arrow::datatypes::IntervalUnit)->Self{
        match interval_unit{
            arrow::datatypes::IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            arrow::datatypes::IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
        }
    }


    pub fn from_i32_to_arrow(interval_unit_i32: i32)->Result<arrow::datatypes::IntervalUnit, BallistaError>{
        let pb_interval_unit = protobuf::IntervalUnit::from_i32(interval_unit_i32);
        use arrow::datatypes::IntervalUnit;
        match pb_interval_unit{
            Some(interval_unit)=>{
                Ok(match interval_unit{
                    protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
                    protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
                })
            }
            None => Err(proto_error("Error converting i32 to DateUnit: Passed invalid variant")),
        }
    }

}

impl protobuf::DateUnit{
    pub fn from_arrow_date_unit(val: &arrow::datatypes::DateUnit)->Self{
        match val{
            arrow::datatypes::DateUnit::Day => protobuf::DateUnit::Day,
            arrow::datatypes::DateUnit::Millisecond => protobuf::DateUnit::DateMillisecond,
        }
    }

    pub fn from_i32_to_arrow(date_unit_i32: i32)->Result<arrow::datatypes::DateUnit, BallistaError>{
        let pb_date_unit = protobuf::DateUnit::from_i32(date_unit_i32);
        use arrow::datatypes::DateUnit;
        match pb_date_unit{
            Some(date_unit)=>{
                Ok(match date_unit{
                    protobuf::DateUnit::Day => DateUnit::Day, 
                    protobuf::DateUnit::DateMillisecond => DateUnit::Millisecond,
                })
            }
            None => Err(proto_error("Error converting i32 to DateUnit: Passed invalid variant")),
        }
    }

}

impl protobuf::TimeUnit{
    pub fn from_arrow_time_unit(val: &arrow::datatypes::TimeUnit)->Self{
        match val{
            arrow::datatypes::TimeUnit::Second => protobuf::TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => protobuf::TimeUnit::TimeMillisecond,
            arrow::datatypes::TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
    pub fn from_i32_to_arrow(time_unit_i32: i32)->Result<arrow::datatypes::TimeUnit, BallistaError>{
        let pb_time_unit = protobuf::TimeUnit::from_i32(time_unit_i32);
        use arrow::datatypes::TimeUnit;
        match pb_time_unit{
            Some(time_unit)=>{
                Ok(match time_unit{
                    protobuf::TimeUnit::Second => TimeUnit::Second,
                    protobuf::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
                    protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
                    protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                })
            }
            None => Err(proto_error("Error converting i32 to TimeUnit: Passed invalid variant")),
        }
    }
}


impl From<&arrow::datatypes::Field> for protobuf::Field{
    fn from(field: &arrow::datatypes::Field)->Self{
        protobuf::Field{
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(field.data_type().into())),
            nullable: field.is_nullable(),
            children: Vec::new(),
        }
    }
}

impl From<&arrow::datatypes::DataType> for protobuf::ArrowType{
    fn from(val: &arrow::datatypes::DataType)->protobuf::ArrowType{
        protobuf::ArrowType{
            arrow_type_enum: Some(val.into()),
        }
    }
}



impl TryInto<arrow::datatypes::DataType> for &protobuf::ArrowType{
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        let pb_arrow_type = self.arrow_type_enum.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: ArrowType missing required field 'data_type'"))?;
        use arrow::datatypes::DataType;
        Ok(match pb_arrow_type{
            protobuf::arrow_type::ArrowTypeEnum::None(_) =>  DataType::Null,
            protobuf::arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            protobuf::arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            protobuf::arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            protobuf::arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            protobuf::arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            protobuf::arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            protobuf::arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            protobuf::arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            protobuf::arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            protobuf::arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            protobuf::arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            protobuf::arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            protobuf::arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            protobuf::arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            protobuf::arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            protobuf::arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            protobuf::arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary, 
            protobuf::arrow_type::ArrowTypeEnum::Date32(date_unit_i32) => DataType::Date32(protobuf::DateUnit::from_i32_to_arrow(*date_unit_i32)?), 
            protobuf::arrow_type::ArrowTypeEnum::Date64(date_unit_i32) => DataType::Date64(protobuf::DateUnit::from_i32_to_arrow(*date_unit_i32)?),
            protobuf::arrow_type::ArrowTypeEnum::Duration(time_unit_i32) => DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?),
            protobuf::arrow_type::ArrowTypeEnum::Timestamp(timestamp) => DataType::Timestamp(
                protobuf::TimeUnit::from_i32_to_arrow(timestamp.time_unit)?,
                match timestamp.timezone.is_empty(){
                    true => None,
                    false => Some(timestamp.timezone.to_owned()),
                }
            ),
            protobuf::arrow_type::ArrowTypeEnum::Time32(time_unit_i32) => DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?),
            protobuf::arrow_type::ArrowTypeEnum::Time64(time_unit_i32) => DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit_i32)?),
            protobuf::arrow_type::ArrowTypeEnum::Interval(interval_unit_i32) => DataType::Interval(protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit_i32)?),
            protobuf::arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal{whole, fractional}) => DataType::Decimal(*whole as usize, *fractional as usize),
            protobuf::arrow_type::ArrowTypeEnum::List(boxed_list) => {
                let field_ref = boxed_list.field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: List message was missing required field 'field_type'"))?.as_ref();
                arrow::datatypes::DataType::List(
                    Box::new(field_ref.try_into()?),
                )
            }
            protobuf::arrow_type::ArrowTypeEnum::LargeList(boxed_list) => {
                let field_ref = boxed_list.field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: List message was missing required field 'field_type'"))?.as_ref();
                arrow::datatypes::DataType::LargeList(
                    Box::new(field_ref.try_into()?),
                )
            }
            protobuf::arrow_type::ArrowTypeEnum::FixedSizeList(boxed_list) => {
                let fsl_ref = boxed_list.as_ref();
                let pb_fieldtype = fsl_ref.field_type.as_ref().ok_or_else(|| proto_error("Protobuf deserialization error: FixedSizeList message was missing required field 'field_type'"))?;
                arrow::datatypes::DataType::FixedSizeList(
                    Box::new(pb_fieldtype.as_ref().try_into()?),
                    fsl_ref.list_size,
                )
            },
            protobuf::arrow_type::ArrowTypeEnum::Struct(struct_type) => {
                let fields = struct_type.sub_field_types.iter()
                                            .map(|field| field.try_into())
                                            .collect::<Result<Vec<_>,_>>()?;
                arrow::datatypes::DataType::Struct(fields)
            },
            protobuf::arrow_type::ArrowTypeEnum::Union(union) =>{
                let union_types = union.union_types.iter()
                                            .map(|field| field.try_into())
                                            .collect::<Result<Vec<_>,_>>()?;
                arrow::datatypes::DataType::Union(union_types)
            },
            protobuf::arrow_type::ArrowTypeEnum::Dictionary(boxed_dict) => {
                let dict_ref = boxed_dict.as_ref();
                let pb_key = dict_ref.key.as_ref().ok_or_else(||proto_error("Protobuf deserialization error: Dictionary message was missing required field 'key'"))?;
                let pb_value = dict_ref.value.as_ref().ok_or_else(||proto_error("Protobuf deserialization error: Dictionary message was missing required field 'value'"))?;
                arrow::datatypes::DataType::Dictionary(Box::new(pb_key.as_ref().try_into()?),Box::new(pb_value.as_ref().try_into()?) )
            },
        })
    }
}




impl TryInto<arrow::datatypes::DataType> for &Box<protobuf::List>{
    type Error = BallistaError;
    fn try_into(self) -> Result<arrow::datatypes::DataType, Self::Error> {
        let list_ref = self.as_ref();
        match &list_ref.field_type{
            Some(pb_field) =>{
                let pb_field_ref = pb_field.as_ref();
                let arrow_field: arrow::datatypes::Field =   pb_field_ref.try_into()?;
                Ok(arrow::datatypes::DataType::List(Box::new(arrow_field)))
            },
            None => Err(proto_error("List message missing required field 'field_type'")),
        }
    }
}


impl From<&arrow::datatypes::DataType> for protobuf::arrow_type::ArrowTypeEnum{
    fn from(val: &arrow::datatypes::DataType)->protobuf::arrow_type::ArrowTypeEnum{
        use protobuf::ArrowType;
        use protobuf::arrow_type::ArrowTypeEnum;
        use protobuf::EmptyMessage;
        match val{
            DataType::Null =>ArrowTypeEnum::None(EmptyMessage{}),
            DataType::Boolean =>ArrowTypeEnum::Bool(EmptyMessage{}),
            DataType::Int8 =>ArrowTypeEnum::Int8(EmptyMessage{}),
            DataType::Int16 =>ArrowTypeEnum::Int16(EmptyMessage{}),
            DataType::Int32 =>ArrowTypeEnum::Int32(EmptyMessage{}),
            DataType::Int64 =>ArrowTypeEnum::Int64(EmptyMessage{}),
            DataType::UInt8 =>ArrowTypeEnum::Uint8(EmptyMessage{}),
            DataType::UInt16 =>ArrowTypeEnum::Uint16(EmptyMessage{}),
            DataType::UInt32 =>ArrowTypeEnum::Uint32(EmptyMessage{}),
            DataType::UInt64 =>ArrowTypeEnum::Uint64(EmptyMessage{}),
            DataType::Float16 =>ArrowTypeEnum::Float16(EmptyMessage{}),
            DataType::Float32 =>ArrowTypeEnum::Float32(EmptyMessage{}),
            DataType::Float64 =>ArrowTypeEnum::Float64(EmptyMessage{}),
            DataType::Timestamp(time_unit, timezone ) => ArrowTypeEnum::Timestamp(protobuf::Timestamp{
                time_unit: protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32,
                timezone: timezone.to_owned().unwrap_or_else(String::new)
            }),
            DataType::Date32(date_unit) => ArrowTypeEnum::Date32(
                protobuf::DateUnit::from_arrow_date_unit(date_unit) as i32
            ),
            DataType::Date64(date_unit) => ArrowTypeEnum::Date64(
                protobuf::DateUnit::from_arrow_date_unit(date_unit) as i32
            ),
            DataType::Time32(time_unit) => ArrowTypeEnum::Time32(
                protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32
            ),
            DataType::Time64(time_unit) => ArrowTypeEnum::Time64(
                protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32
            ),
            DataType::Duration(time_unit) => ArrowTypeEnum::Duration(protobuf::TimeUnit::from_arrow_time_unit(time_unit) as i32),
            DataType::Interval(interval_unit) => ArrowTypeEnum::Interval(protobuf::IntervalUnit::from_arrow_interval_unit(interval_unit) as i32),
            DataType::Binary => ArrowTypeEnum::Binary(EmptyMessage{}),
            DataType::FixedSizeBinary(size) => ArrowTypeEnum::FixedSizeBinary(*size),
            DataType::LargeBinary => ArrowTypeEnum::LargeBinary(EmptyMessage{}),
            DataType::Utf8 => ArrowTypeEnum::Utf8(EmptyMessage{}),
            DataType::LargeUtf8 => ArrowTypeEnum::LargeUtf8(EmptyMessage{}),
            DataType::List(item_type) => ArrowTypeEnum::List(Box::new(protobuf::List{
                field_type: Some(Box::new(item_type.as_ref().into()))
            })),
            DataType::FixedSizeList(item_type, size ) => ArrowTypeEnum::FixedSizeList(Box::new(protobuf::FixedSizeList{
                field_type: Some(Box::new(item_type.as_ref().into())),
                list_size : *size,
            })),
            DataType::LargeList(item_type) => ArrowTypeEnum::LargeList(Box::new(
                protobuf::List{
                    field_type: Some(Box::new(item_type.as_ref().into())),
                }
            )),
            DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(protobuf::Struct{
                sub_field_types: struct_fields.iter()
                                            .map(|field| {
                                                field.into()
                                            })
                                            .collect::<Vec<_>>(),
            }),
            DataType::Union(union_types) => ArrowTypeEnum::Union(protobuf::Union{
                union_types: union_types.iter()
                .map(|field| {
                    field.into()
                })
                .collect::<Vec<_>>(),
            }),
            DataType::Dictionary(key_type, value_type) => ArrowTypeEnum::Dictionary(Box::new(protobuf::Dictionary{
                key: Some(Box::new(key_type.as_ref().into())),
                value: Some(Box::new(value_type.as_ref().into())), 
            })),
            DataType::Decimal(whole, fractional) => ArrowTypeEnum::Decimal(protobuf::Decimal{whole:*whole as u64, fractional: *fractional as u64})
        }
    }
}

use std::convert::TryFrom;

//Does not check if list subtypes are valid
fn is_valid_scalar_type_no_list_check(datatype: &arrow::datatypes::DataType)->bool{
    match datatype{
        DataType::Boolean   | 
        DataType::Int8      |
        DataType::Int16     |
        DataType::Int32     |
        DataType::Int64     |
        DataType::UInt8     |
        DataType::UInt16    |   
        DataType::UInt32    |   
        DataType::UInt64    |   
        DataType::Float32   |   
        DataType::Float64   |
        DataType::LargeUtf8 |
        DataType::Utf8      => true, 
        DataType::Date32(date_unit) =>{
            match date_unit{
                arrow::datatypes::DateUnit::Day => true,
                _=> false,
            }
        },
        DataType::Time64(time_unit) => match time_unit{
            arrow::datatypes::TimeUnit::Microsecond |
            arrow::datatypes::TimeUnit::Nanosecond => true,
            _ => false,
        },

        DataType::List(_) => {
            true
        },
        _ => false,
    }
}



impl TryFrom<&arrow::datatypes::DataType> for protobuf::scalar_type::Datatype{
    type Error = BallistaError;
    fn try_from(val: &arrow::datatypes::DataType)-> Result<Self, Self::Error>{
        use protobuf::{List, PrimitiveScalarType};
        use protobuf::Field;
        use protobuf::scalar_type;
        use arrow::datatypes::DateUnit;
        let scalar_value = match val{
            DataType::Boolean => scalar_type::Datatype::Scalar(PrimitiveScalarType::Bool as i32),
            DataType::Null => scalar_type::Datatype::Scalar(PrimitiveScalarType::Null as i32),
            DataType::Int8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int8 as i32),
            DataType::Int16 =>  scalar_type::Datatype::Scalar(PrimitiveScalarType::Int16 as i32),
            DataType::Int32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int32 as i32),
            DataType::Int64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Int64 as i32),
            DataType::UInt8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint8 as i32),
            DataType::UInt16 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint16 as i32),
            DataType::UInt32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint32 as i32),
            DataType::UInt64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Uint64 as i32),
            DataType::Float32 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float32 as i32),
            DataType::Float64 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Float64 as i32),
            DataType::Date32(date_unit) =>  match  date_unit {
                 DateUnit::Day => scalar_type::Datatype::Scalar(PrimitiveScalarType::Date32 as i32),
                    _ => return Err(proto_error("Found invalid date unit for scalar value, only DateUnit::Day is allowed")),
            },
            DataType::Time64(time_unit) => match time_unit{
                arrow::datatypes::TimeUnit::Microsecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeMicrosecond as i32),
                arrow::datatypes::TimeUnit::Nanosecond => scalar_type::Datatype::Scalar(PrimitiveScalarType::TimeNanosecond as i32),
                _ => return Err(proto_error(format!("Found invalid time unit for scalar value, only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units: {:?}", time_unit))),
            },
            DataType::Utf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::Utf8 as i32),
            DataType::LargeUtf8 => scalar_type::Datatype::Scalar(PrimitiveScalarType::LargeUtf8 as i32),
            DataType::List(field_type) => {
                let mut field_names: Vec<String> = Vec::new(); 
                let mut curr_field: &arrow::datatypes::Field = field_type.as_ref();
                field_names.push(curr_field.name().to_owned());
                let mut depth: u64 = 0;
                //For each nested field check nested datatype, since datafusion scalars only support recursive lists with a leaf scalar type
                // any other compound types are errors.
                 
                while let DataType::List(nested_field_type) = curr_field.data_type(){
                    curr_field = nested_field_type.as_ref();
                    field_names.push(curr_field.name().to_owned());
                    depth = depth + 1;
                    if !is_valid_scalar_type_no_list_check(curr_field.data_type()){
                        return Err(proto_error(format!("{:?} is an invalid scalar type", curr_field)));
                    }
                }
                let deepest_datatype = curr_field.data_type();
                if !is_valid_scalar_type_no_list_check(deepest_datatype){
                    return Err(proto_error(format!("The list nested type {:?} is an invalid scalar type", curr_field)));
                }
                let pb_deepest_type: PrimitiveScalarType = match deepest_datatype{
                    DataType::Boolean => PrimitiveScalarType::Bool,
                    DataType::Int8 => PrimitiveScalarType::Int8,
                    DataType::Int16 => PrimitiveScalarType::Int16,
                    DataType::Int32 => PrimitiveScalarType::Int32,
                    DataType::Int64 => PrimitiveScalarType::Int64,
                    DataType::UInt8 => PrimitiveScalarType::Uint8,
                    DataType::UInt16 => PrimitiveScalarType::Uint16,
                    DataType::UInt32 => PrimitiveScalarType::Uint32,
                    DataType::UInt64 => PrimitiveScalarType::Uint64,
                    DataType::Float32 =>  PrimitiveScalarType::Float32,
                    DataType::Float64 =>  PrimitiveScalarType::Float64,
                    DataType::Date32(_) =>  PrimitiveScalarType::Date32,
                    DataType::Time64(time_unit) =>{
                        match time_unit{
                            arrow::datatypes::TimeUnit::Microsecond => PrimitiveScalarType::TimeMicrosecond,
                            arrow::datatypes::TimeUnit::Nanosecond => PrimitiveScalarType::TimeNanosecond,
                            _=> unreachable!(),
                        }
                    },
                    
                    DataType::Utf8 => PrimitiveScalarType::Utf8,
                    DataType::LargeUtf8 => PrimitiveScalarType::LargeUtf8,
                    _=> unreachable!()
                };
                protobuf::scalar_type::Datatype::List(protobuf::ScalarListType{
                    field_names,
                    depth,
                    deepest_type: pb_deepest_type as i32,
                })
            },
            _ => Err(proto_error(format!("Error converting to Datatype to scalar type, {:?} is invalid as a datafusion scalar.", val)))?, 
        };
        Ok(scalar_value)
    }
} 





impl TryFrom<&datafusion::scalar::ScalarValue> for protobuf::ScalarValue{
    type Error = BallistaError;
    fn try_from(val: &datafusion::scalar::ScalarValue)->Result<protobuf::ScalarValue, Self::Error>{
        use protobuf::scalar_value::Value;
        use protobuf::PrimitiveScalarType;
        use datafusion::scalar;
        Ok(match val{
            scalar::ScalarValue::Boolean(val) => create_proto_scalar(val, PrimitiveScalarType::Bool, |s| Value::BoolValue(*s) ),
            scalar::ScalarValue::Float32(val) => create_proto_scalar(val, PrimitiveScalarType::Float32, |s| Value::Float32Value(*s) ),
            scalar::ScalarValue::Float64(val) => create_proto_scalar(val, PrimitiveScalarType::Float64, |s| Value::Float64Value(*s) ),
            scalar::ScalarValue::Int8(val) => create_proto_scalar(val, PrimitiveScalarType::Int8, |s| Value::Int8Value(*s as i32) ),
            scalar::ScalarValue::Int16(val) => create_proto_scalar(val, PrimitiveScalarType::Int16, |s| Value::Int16Value(*s as i32) ),
            scalar::ScalarValue::Int32(val) => create_proto_scalar(val, PrimitiveScalarType::Int32, |s| Value::Int32Value(*s) ),
            scalar::ScalarValue::Int64(val) => create_proto_scalar(val, PrimitiveScalarType::Int64, |s| Value::Int64Value(*s) ),
            scalar::ScalarValue::UInt8(val) => create_proto_scalar(val, PrimitiveScalarType::Uint8, |s| Value::Uint8Value(*s as u32) ),
            scalar::ScalarValue::UInt16(val) => create_proto_scalar(val, PrimitiveScalarType::Uint16, |s| Value::Uint16Value(*s as u32) ),
            scalar::ScalarValue::UInt32(val) => create_proto_scalar(val, PrimitiveScalarType::Uint32, |s| Value::Uint32Value(*s ) ),
            scalar::ScalarValue::UInt64(val) => create_proto_scalar(val, PrimitiveScalarType::Uint64, |s| Value::Uint64Value(*s) ),
            scalar::ScalarValue::Utf8(val) => create_proto_scalar(val, PrimitiveScalarType::Utf8, |s| Value::Utf8Value(s.to_owned()) ),
            scalar::ScalarValue::LargeUtf8(val) => create_proto_scalar(val, PrimitiveScalarType::LargeUtf8, |s| Value::LargeUtf8Value(s.to_owned())),
            scalar::ScalarValue::List(value, datatype) => {
                println!("Current datatype of list: {:?}", datatype);
                match value{
                    Some(values) =>{
                        if values.len() == 0{
                            protobuf::ScalarValue{
                                value: Some(protobuf::scalar_value::Value::ListValue( protobuf::ScalarListValue{
                                    datatype: Some(datatype.try_into()?),
                                    values: Vec::new(),
                                }))
                            }
                        }else{
                            let scalar_type = match datatype{
                                DataType::List(field) => field.as_ref().data_type(),
                                _ => todo!("Proper error handling"),
                            };
                            println!("Current scalar type for list: {:?}", scalar_type);
                            let type_checked_values: Vec<protobuf::ScalarValue> = values.iter()
                                                                                    .map(|scalar|
                                                                                        match (scalar, scalar_type){
                                                                                            (scalar::ScalarValue::List(_,list_datatype), arrow::datatypes::DataType::List(field)) => {
                                                                                                let scalar_datatype = field.data_type();
                                                                                                if std::mem::discriminant(list_datatype) != std::mem::discriminant(scalar_datatype){
                                                                                                    return Err(proto_error(format!("Protobuf serialization error: Lists with inconsistent typing {:?} and {:?} found within list", list_datatype, scalar_datatype)));
                                                                                                }
                                                                                                scalar.try_into()
                                                                                            },
                                                                                            (scalar::ScalarValue::Boolean(_), arrow::datatypes::DataType::Boolean) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Float32(_), arrow::datatypes::DataType::Float32) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Float64(_), arrow::datatypes::DataType::Float64) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Int8(_), arrow::datatypes::DataType::Int8) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Int16(_), arrow::datatypes::DataType::Int16) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Int32(_), arrow::datatypes::DataType::Int32) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Int64(_), arrow::datatypes::DataType::Int64) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::UInt8(_), arrow::datatypes::DataType::UInt8) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::UInt16(_), arrow::datatypes::DataType::UInt16) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::UInt32(_), arrow::datatypes::DataType::UInt32) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::UInt64(_), arrow::datatypes::DataType::UInt64) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::Utf8(_), arrow::datatypes::DataType::Utf8) => scalar.try_into(),
                                                                                            (scalar::ScalarValue::LargeUtf8(_), arrow::datatypes::DataType::LargeUtf8) => scalar.try_into(),
                                                                                            _=> Err(proto_error(format!("Protobuf serialization error, {:?} was inconsistent with designated type {:?}", scalar, datatype))),
                                                                                                
                                                                                            }).collect::<Result<Vec<_>, _>>()?;
                            protobuf::ScalarValue{
                                value: Some(
                                    protobuf::scalar_value::Value::ListValue(
                                        protobuf::ScalarListValue{
                                            datatype: Some(datatype.try_into()?),
                                            values: type_checked_values,
                                        }
                                    )
                                )
                            }
                        }
                    },
                    None => protobuf::ScalarValue{
                        value: Some(protobuf::scalar_value::Value::NullListValue( datatype.try_into()?))
                    }
                }
            },
            datafusion::scalar::ScalarValue::Date32(val) => create_proto_scalar(val, PrimitiveScalarType::Date32, |s| Value::Date32Value(*s) ),
            datafusion::scalar::ScalarValue::TimeMicrosecond(val) => create_proto_scalar(val, PrimitiveScalarType::TimeMicrosecond, |s| Value::TimeMicrosecondValue(*s) ),
            datafusion::scalar::ScalarValue::TimeNanosecond(val) => create_proto_scalar(val, PrimitiveScalarType::TimeNanosecond, |s| Value::TimeNanosecondValue(*s) ),
        })
    }
}

impl TryInto<protobuf::LogicalPlanNode> for &LogicalPlan {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::LogicalPlanNode, Self::Error> {
        match self {
            LogicalPlan::TableScan {
                table_name,
                source,
                projected_schema,
                filters,
                ..
            } => {
                let schema = source.schema();
                let source = source.as_any();
                let columns = projected_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect();
                let projection = Some(protobuf::ProjectionColumns { columns });
                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let mut node = empty_logical_plan_node();

                if let Some(parquet) = source.downcast_ref::<ParquetTable>() {
                    node.parquet_scan = Some(protobuf::ParquetTableScanNode {
                        table_name: table_name.to_owned(),
                        path: parquet.path().to_owned(),
                        projection,
                        schema: Some(schema),
                        filters,
                    });
                    Ok(node)
                } else if let Some(csv) = source.downcast_ref::<CsvFile>() {
                    node.csv_scan = Some(protobuf::CsvTableScanNode {
                        table_name: table_name.to_owned(),
                        path: csv.path().to_owned(),
                        projection,
                        schema: Some(schema),
                        has_header: csv.has_header(),
                        delimiter: csv.delimiter().to_string(),
                        file_extension: csv.file_extension().to_string(),
                        filters,
                    });
                    Ok(node)
                } else {
                    Err(BallistaError::General(format!(
                        "logical plan to_proto unsupported table provider {:?}",
                        source
                    )))
                }
            }
            LogicalPlan::Projection { expr, input, .. } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.projection = Some(protobuf::ProjectionNode {
                    expr: expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            LogicalPlan::Filter { predicate, input } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.selection = Some(protobuf::SelectionNode {
                    expr: Some(predicate.try_into()?),
                });
                Ok(node)
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.aggregate = Some(protobuf::AggregateNode {
                    group_expr: group_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    aggr_expr: aggr_expr
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                });
                Ok(node)
            }
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                ..
            } => {
                let left: protobuf::LogicalPlanNode = left.as_ref().try_into()?;
                let right: protobuf::LogicalPlanNode = right.as_ref().try_into()?;
                let join_type = match join_type {
                    JoinType::Inner => protobuf::JoinType::Inner,
                    JoinType::Left => protobuf::JoinType::Left,
                    JoinType::Right => protobuf::JoinType::Right,
                };
                let left_join_column = on.iter().map(|on| on.0.to_owned()).collect();
                let right_join_column = on.iter().map(|on| on.1.to_owned()).collect();
                let mut node = empty_logical_plan_node();
                node.join = Some(Box::new(protobuf::JoinNode {
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                    join_type: join_type.into(),
                    left_join_column,
                    right_join_column,
                }));
                Ok(node)
            }
            LogicalPlan::Limit { input, n } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                node.limit = Some(protobuf::LimitNode { limit: *n as u32 });
                Ok(node)
            }
            LogicalPlan::Sort { input, expr } => {
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                node.sort = Some(protobuf::SortNode {
                    expr: selection_expr,
                });
                Ok(node)
            }
            LogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => {
                use datafusion::logical_plan::Partitioning;
                let input: protobuf::LogicalPlanNode = input.as_ref().try_into()?;
                let mut node = empty_logical_plan_node();
                node.input = Some(Box::new(input));

                //Assumed common usize field was batch size
                //Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, batch_size) => {
                        PartitionMethod::Hash(protobuf::HashRepartition {
                            hash_expr: exprs.iter().map(|expr| expr.try_into()).collect::<Result<
                                Vec<_>,
                                BallistaError,
                            >>(
                            )?,
                            batch_size: *batch_size as u64,
                        })
                    }
                    Partitioning::RoundRobinBatch(batch_size) => {
                        PartitionMethod::RoundRobin(*batch_size as u64)
                    }
                };

                node.repartition = Some(protobuf::RepartitionNode {
                    partition_method: Some(pb_partition_method),
                });

                Ok(node)
            }
            LogicalPlan::EmptyRelation {
                produce_one_row, ..
            } => {
                let mut node = empty_logical_plan_node();
                node.empty_relation = Some(protobuf::EmptyRelationNode {
                    produce_one_row: *produce_one_row,
                });
                Ok(node)
            }
            LogicalPlan::CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                schema: df_schema,
            } => {
                let mut node = empty_logical_plan_node();
                use datafusion::sql::parser::FileType;
                let schema: Schema = df_schema.as_ref().clone().into();
                let pb_schema: protobuf::Schema = (&schema).try_into().map_err(|e| {
                    BallistaError::General(format!(
                        "Could not convert schema into protobuf: {:?}",
                        e
                    ))
                })?;

                let pb_file_type: protobuf::FileType = match file_type {
                    FileType::NdJson => protobuf::FileType::NdJson,
                    FileType::Parquet => protobuf::FileType::Parquet,
                    FileType::CSV => protobuf::FileType::Csv,
                };

                node.create_external_table = Some(protobuf::CreateExternalTableNode {
                    name: name.clone(),
                    location: location.clone(),
                    file_type: pb_file_type as i32,
                    has_header: *has_header,
                    schema: Some(pb_schema),
                });
                Ok(node)
            }
            LogicalPlan::Explain { verbose, plan, .. } => {
                let mut node = empty_logical_plan_node();
                let input: protobuf::LogicalPlanNode = plan.as_ref().try_into()?;
                node.input = Some(Box::new(input));
                node.explain = Some(protobuf::ExplainNode { verbose: *verbose });
                Ok(node)
            }
            LogicalPlan::Extension { .. } => unimplemented!(),
            // _ => Err(BallistaError::General(format!(
            //     "logical plan to_proto {:?}",
            //     self
            // ))),
        }
    }
}

fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: protobuf::PrimitiveScalarType,
    constructor: T,
) -> protobuf::ScalarValue {
    protobuf::ScalarValue{
    value: Some(v.as_ref()
                .map(constructor)
                .unwrap_or(protobuf::scalar_value::Value::NullValue(null_arrow_type as i32)))
    }
}



fn create_proto_scalar_expr_node<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: protobuf::PrimitiveScalarType,
    constructor: T,
) -> protobuf::LogicalExprNode {
    use protobuf::logical_expr_node::ExprType;
    let val = v.as_ref()
        .map(constructor)
        .unwrap_or(
        protobuf::scalar_value::Value::NullValue(null_arrow_type as i32)
        );
        protobuf::LogicalExprNode{
            expr_type: Some(ExprType::Literal(protobuf::ScalarValue{
                value: Some(val),
            }))
        }
}


impl TryInto<protobuf::LogicalExprNode> for &Expr {
    type Error = BallistaError;
    
    fn try_into(self) -> Result<protobuf::LogicalExprNode, Self::Error> {
        use protobuf::scalar_value::Value;
        use datafusion::scalar::ScalarValue;
        match self {
            Expr::Column(name) => {
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::ColumnName(name.clone())),
                };
                Ok(expr)
            }
            Expr::Alias(expr, alias) => {
                let alias = Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                });
                let expr = protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Alias(alias)),
                };
                Ok(expr)
            }
            Expr::Literal(value) => match value {
                ScalarValue::Utf8(s) => Ok(
                    create_proto_scalar_expr_node(s, protobuf::PrimitiveScalarType::Utf8, |s| {
                        Value::Utf8Value(s.to_owned())} 
                    )),

                    ScalarValue::LargeUtf8(s)=> Ok(
                        create_proto_scalar_expr_node(s, protobuf::PrimitiveScalarType::LargeUtf8, |s| {
                            Value::LargeUtf8Value(s.to_owned())} 
                        )),
                ScalarValue::Int8(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Int8, |s| {
                        Value::Int8Value(*s as i32)} 
                    )),
                ScalarValue::Int16(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Int16, |s| {
                        Value::Int16Value(*s as i32)} 
                    )),
                ScalarValue::Int32(n) =>Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Int32, |s| {
                        Value::Int32Value(*s)} 
                    )),
                ScalarValue::Int64(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Int64, |s| {
                        Value::Int64Value(*s)} 
                    )),
                ScalarValue::UInt8(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Uint8, |s| {
                        Value::Uint8Value(*s as u32)} 
                    )),
                ScalarValue::UInt16(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Uint16, |s| {
                        Value::Uint16Value(*s as u32)} 
                    )),
                ScalarValue::UInt32(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Uint32, |s| {
                        Value::Uint32Value(*s)} 
                    )),
                ScalarValue::UInt64(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Uint64, |s| {
                        Value::Uint64Value(*s )} 
                    )),
                ScalarValue::Float32(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Float32, |s| {
                        Value::Float32Value(*s )} 
                    )),
                ScalarValue::Float64(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Float64, |s| {
                        Value::Float64Value(*s )} 
                    )),
                ScalarValue::Date32(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::Date32, |s| {
                        Value::Date32Value(*s )} 
                    )),
                
                ScalarValue::Boolean(b) => Ok(
                    create_proto_scalar_expr_node(b, protobuf::PrimitiveScalarType::Bool, |s|{
                        Value::BoolValue(*s)
                    })
                ),
                
                ScalarValue::List(list,  datatype) => {
                    let list_scalar_value: protobuf::scalar_value::Value = match list{
                        None => {
                             protobuf::scalar_value::Value::NullListValue(protobuf::ScalarType::try_from(datatype)?)
                        },
                        //TODO add type checks on outbound list values
                        Some(value)=>{
                            let scalar_values: Vec<protobuf::ScalarValue> = value.iter()
                                .map(|scalar| scalar.try_into())
                                .collect::<Result<Vec<_>, _>>()?;

                            protobuf::scalar_value::Value::ListValue(
                                protobuf::ScalarListValue{
                                    datatype: Some(protobuf::ScalarType::try_from(datatype)?),
                                    values: scalar_values,
                                }
                            )
                        }
                    };

                    Ok(protobuf::LogicalExprNode{
                        expr_type: Some(protobuf::logical_expr_node::ExprType::Literal(
                            protobuf::ScalarValue{
                                value: Some(list_scalar_value),
                            }
                        ))
                    })
                },
                ScalarValue::TimeMicrosecond(n) => Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::TimeMicrosecond, |s|{
                        Value::TimeMicrosecondValue(*s)
                    })
                ),
                ScalarValue::TimeNanosecond(n) =>Ok(
                    create_proto_scalar_expr_node(n, protobuf::PrimitiveScalarType::TimeNanosecond, |s|{
                        Value::TimeNanosecondValue(*s)
                    })
                ),
            },
            Expr::BinaryExpr { left, op, right } => {
                let binary_expr = Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::BinaryExpr(binary_expr)),
                })
            }
            Expr::AggregateFunction {
                ref fun, ref args, ..
            } => {
                let aggr_function = match fun {
                    AggregateFunction::Min => protobuf::AggregateFunction::Min,
                    AggregateFunction::Max => protobuf::AggregateFunction::Max,
                    AggregateFunction::Sum => protobuf::AggregateFunction::Sum,
                    AggregateFunction::Avg => protobuf::AggregateFunction::Avg,
                    AggregateFunction::Count => protobuf::AggregateFunction::Count,
                };

                let arg = &args[0];
                let aggregate_expr = Box::new(protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: Some(Box::new(arg.try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
                })
            }
            Expr::ScalarVariable(_) => unimplemented!(),
            Expr::ScalarFunction { .. } => unimplemented!(),
            Expr::ScalarUDF { .. } => unimplemented!(),
            Expr::AggregateUDF { .. } => unimplemented!(),
            Expr::Not(expr) => {
                let expr = Box::new(protobuf::Not {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::NotExpr(expr)),
                })
            }
            Expr::IsNull(expr) => {
                let expr = Box::new(protobuf::IsNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNullExpr(expr)),
                })
            }
            Expr::IsNotNull(expr) => {
                let expr = Box::new(protobuf::IsNotNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::IsNotNullExpr(expr)),
                })
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Box::new(protobuf::BetweenNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    negated: *negated,
                    low: Some(Box::new(low.as_ref().try_into()?)),
                    high: Some(Box::new(high.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Between(expr)),
                })
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        Ok(protobuf::WhenThen {
                            when_expr: Some(w.as_ref().try_into()?),
                            then_expr: Some(t.as_ref().try_into()?),
                        })
                    })
                    .collect::<Result<Vec<protobuf::WhenThen>, BallistaError>>()?;
                let expr = Box::new(protobuf::CaseNode {
                    expr: match expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                    when_then_expr,
                    else_expr: match else_expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Case(expr)),
                })
            }
            Expr::Cast { expr, data_type } => {
                let expr = Box::new(protobuf::CastNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    arrow_type: Some(data_type.into()),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Cast(expr)),
                })
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = Box::new(protobuf::SortExprNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Sort(expr)),
                })
            }
            Expr::Negative(expr) => {
                let expr = Box::new(protobuf::NegativeNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::Negative(expr)),
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Box::new(protobuf::InListNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    list: list
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, BallistaError>>()?,
                    negated: *negated,
                });
                Ok(protobuf::LogicalExprNode {
                    expr_type: Some(protobuf::logical_expr_node::ExprType::InList(expr)),
                })
            }
            Expr::Wildcard => Ok(protobuf::LogicalExprNode {
                expr_type: Some(protobuf::logical_expr_node::ExprType::Wildcard(true)),
            }),
            // _ => Err(BallistaError::General(format!(
            //     "logical expr to_proto {:?}",
            //     self
            // ))),
        }
    }
}


impl TryInto<protobuf::Schema> for &Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Schema, Self::Error> {
        Ok(protobuf::Schema {
            columns: self
                .fields()
                .iter()
                .map(|field| {
                    let proto =protobuf::ArrowType::from(field.data_type());
                    protobuf::Field{
                        name: field.name().to_owned(),
                        arrow_type: Some(Box::new(proto.into())),
                        nullable: field.is_nullable(),
                        children: vec![],
                    }
                    
                })
                .collect::<Vec<_>>(),
        })
    }
}

impl TryFrom<&arrow::datatypes::DataType> for protobuf::ScalarType{
    type Error =  BallistaError;
    fn try_from(value: &arrow::datatypes::DataType) -> Result<Self, Self::Error> {
        let datatype = protobuf::scalar_type::Datatype::try_from(value)?;
        Ok(protobuf::ScalarType{
            datatype: Some(datatype),
        })
    }
}