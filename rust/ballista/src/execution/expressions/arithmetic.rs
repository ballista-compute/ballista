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

use std::sync::Arc;

use crate::arrow::array;
use crate::arrow::compute;
use crate::arrow::datatypes::{DataType, Schema};
use crate::cast_array;
use crate::error::{ballista_error, Result};
use crate::execution::physical_plan::{ColumnarBatch, ColumnarValue, Expression};

macro_rules! binary_op {
    ($LEFT:ident, $RIGHT:ident, $TY:ident, $OP:ident) => {{
        let l = cast_array!($LEFT, $TY)?;
        let r = cast_array!($RIGHT, $TY)?;
        Ok(ColumnarValue::Columnar(Arc::new(compute::$OP(l, r)?)))
    }};
}

#[derive(Debug)]
pub struct Add {
    l: Arc<dyn Expression>,
    r: Arc<dyn Expression>,
}

impl Add {
    pub fn new(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Self {
        Self { l, r }
    }
}

pub fn add(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Arc<dyn Expression> {
    Arc::new(Add::new(l, r))
}

impl Expression for Add {
    fn name(&self) -> String {
        format!("{} + {}", self.l.name(), self.r.name())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.l.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow();
        let r = self.r.evaluate(input)?.to_arrow();
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Add expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int8 => binary_op!(l, r, Int8Array, add),
            DataType::Int16 => binary_op!(l, r, Int16Array, add),
            DataType::Int32 => binary_op!(l, r, Int32Array, add),
            DataType::Int64 => binary_op!(l, r, Int64Array, add),
            DataType::UInt8 => binary_op!(l, r, UInt8Array, add),
            DataType::UInt16 => binary_op!(l, r, UInt16Array, add),
            DataType::UInt32 => binary_op!(l, r, UInt32Array, add),
            DataType::UInt64 => binary_op!(l, r, UInt64Array, add),
            DataType::Float32 => binary_op!(l, r, Float32Array, add),
            DataType::Float64 => binary_op!(l, r, Float64Array, add),
            _ => Err(ballista_error("Unsupported datatype for Add expression")),
        }
    }
}

#[derive(Debug)]
pub struct Subtract {
    l: Arc<dyn Expression>,
    r: Arc<dyn Expression>,
}

impl Subtract {
    pub fn new(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Self {
        Self { l, r }
    }
}

pub fn subtract(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Arc<dyn Expression> {
    Arc::new(Subtract::new(l, r))
}

impl Expression for Subtract {
    fn name(&self) -> String {
        format!("{} - {}", self.l.name(), self.r.name())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.l.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow();
        let r = self.r.evaluate(input)?.to_arrow();
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Subtract expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int8 => binary_op!(l, r, Int8Array, subtract),
            DataType::Int16 => binary_op!(l, r, Int16Array, subtract),
            DataType::Int32 => binary_op!(l, r, Int32Array, subtract),
            DataType::Int64 => binary_op!(l, r, Int64Array, subtract),
            DataType::UInt8 => binary_op!(l, r, UInt8Array, subtract),
            DataType::UInt16 => binary_op!(l, r, UInt16Array, subtract),
            DataType::UInt32 => binary_op!(l, r, UInt32Array, subtract),
            DataType::UInt64 => binary_op!(l, r, UInt64Array, subtract),
            DataType::Float32 => binary_op!(l, r, Float32Array, subtract),
            DataType::Float64 => binary_op!(l, r, Float64Array, subtract),
            _ => Err(ballista_error(
                "Unsupported datatype for Subtract expression",
            )),
        }
    }
}

#[derive(Debug)]
pub struct Multiply {
    l: Arc<dyn Expression>,
    r: Arc<dyn Expression>,
}

impl Multiply {
    pub fn new(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Self {
        Self { l, r }
    }
}

pub fn mult(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Arc<dyn Expression> {
    Arc::new(Multiply::new(l, r))
}

impl Expression for Multiply {
    fn name(&self) -> String {
        format!("{} - {}", self.l.name(), self.r.name())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.l.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow();
        let r = self.r.evaluate(input)?.to_arrow();
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Multiply expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int8 => binary_op!(l, r, Int8Array, multiply),
            DataType::Int16 => binary_op!(l, r, Int16Array, multiply),
            DataType::Int32 => binary_op!(l, r, Int32Array, multiply),
            DataType::Int64 => binary_op!(l, r, Int64Array, multiply),
            DataType::UInt8 => binary_op!(l, r, UInt8Array, multiply),
            DataType::UInt16 => binary_op!(l, r, UInt16Array, multiply),
            DataType::UInt32 => binary_op!(l, r, UInt32Array, multiply),
            DataType::UInt64 => binary_op!(l, r, UInt64Array, multiply),
            DataType::Float32 => binary_op!(l, r, Float32Array, multiply),
            DataType::Float64 => binary_op!(l, r, Float64Array, multiply),
            _ => Err(ballista_error(
                "Unsupported datatype for Multiply expression",
            )),
        }
    }
}

#[derive(Debug)]
pub struct Divide {
    l: Arc<dyn Expression>,
    r: Arc<dyn Expression>,
}

impl Divide {
    pub fn new(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Self {
        Self { l, r }
    }
}

pub fn div(l: Arc<dyn Expression>, r: Arc<dyn Expression>) -> Arc<dyn Expression> {
    Arc::new(Divide::new(l, r))
}

impl Expression for Divide {
    fn name(&self) -> String {
        format!("{} - {}", self.l.name(), self.r.name())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.l.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        //TODO
        Ok(true)
    }

    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue> {
        let l = self.l.evaluate(input)?.to_arrow();
        let r = self.r.evaluate(input)?.to_arrow();
        if l.data_type() != r.data_type() {
            return Err(ballista_error(
                "Both inputs to Divide expression must have same type",
            ));
        }
        match l.data_type() {
            DataType::Int8 => binary_op!(l, r, Int8Array, divide),
            DataType::Int16 => binary_op!(l, r, Int16Array, divide),
            DataType::Int32 => binary_op!(l, r, Int32Array, divide),
            DataType::Int64 => binary_op!(l, r, Int64Array, divide),
            DataType::UInt8 => binary_op!(l, r, UInt8Array, divide),
            DataType::UInt16 => binary_op!(l, r, UInt16Array, divide),
            DataType::UInt32 => binary_op!(l, r, UInt32Array, divide),
            DataType::UInt64 => binary_op!(l, r, UInt64Array, divide),
            DataType::Float32 => binary_op!(l, r, Float32Array, divide),
            DataType::Float64 => binary_op!(l, r, Float64Array, divide),
            _ => Err(ballista_error("Unsupported datatype for Divide expression")),
        }
    }
}
