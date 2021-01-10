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

//! Data generator used in unit and integration tests

use std::sync::Arc;

use crate::arrow::array::{self, ArrayRef};
use crate::arrow::datatypes::{DataType, Schema};
use crate::error::Result;
use crate::execution::physical_plan::ColumnarBatch;

use arrow::record_batch::RecordBatch;
use random_fast_rng::{FastRng, Random};

/// Random data generator
#[allow(dead_code)]
pub struct DataGen {
    rng: FastRng,
}

impl Default for DataGen {
    fn default() -> Self {
        DataGen::new()
    }
}

macro_rules! build_primitive_array {
    ($SELF:ident, $ARRAY_TYPE:ident, $TY:ty, $NULLABLE:expr, $LEN:expr) => {{
        let mut builder = array::$ARRAY_TYPE::builder($LEN);
        for _ in 0..$LEN {
            if $NULLABLE && $SELF.rng.get_u8() < 10 {
                builder.append_null()?;
            } else {
                builder.append_value($SELF.rng.get_u64() as $TY)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

#[allow(dead_code)]
impl DataGen {
    /// Create a data generator using a fixed seed for reproducible data.
    pub fn new() -> Self {
        Self {
            rng: FastRng::seed(0, 0),
        }
    }

    /// Generate an Array array with the specified data type and length
    fn create_array(
        &mut self,
        data_type: &DataType,
        nullable: bool,
        len: usize,
    ) -> Result<ArrayRef> {
        match data_type {
            DataType::Int8 => build_primitive_array!(self, Int8Array, i8, nullable, len),
            DataType::Int16 => build_primitive_array!(self, Int16Array, i16, nullable, len),
            DataType::Int32 => build_primitive_array!(self, Int32Array, i32, nullable, len),
            DataType::Int64 => build_primitive_array!(self, Int64Array, i64, nullable, len),
            DataType::UInt8 => build_primitive_array!(self, UInt8Array, u8, nullable, len),
            DataType::UInt16 => build_primitive_array!(self, UInt16Array, u16, nullable, len),
            DataType::UInt32 => build_primitive_array!(self, UInt32Array, u32, nullable, len),
            DataType::UInt64 => build_primitive_array!(self, UInt64Array, u64, nullable, len),
            DataType::Float32 => build_primitive_array!(self, Float32Array, f32, nullable, len),
            DataType::Float64 => build_primitive_array!(self, Float64Array, f64, nullable, len),
            _ => unimplemented!(),
        }
    }

    /// Generate a columnar batch with the specified schema and length
    pub fn create_batch(&mut self, schema: &Schema, len: usize) -> Result<ColumnarBatch> {
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| self.create_array(f.data_type(), f.is_nullable(), len))
            .collect::<Result<Vec<_>>>()?;

        Ok(ColumnarBatch::from_arrow(&RecordBatch::try_new(
            Arc::new(schema.clone()),
            columns,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::Field;

    #[test]
    fn create_primitive_array() -> Result<()> {
        let mut gen = DataGen::new();
        let array = gen.create_array(&DataType::Int64, true, 8)?;
        assert_eq!(8, array.len());
        let array = array
            .as_any()
            .downcast_ref::<array::Int64Array>()
            .expect("cast failed");
        assert_eq!(6650325416439211286, array.value(0));
        assert_eq!(5068474728774271781, array.value(7));
        Ok(())
    }

    #[test]
    fn create_primitive_batch() -> Result<()> {
        let mut gen = DataGen::new();
        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, false),
        ]);
        let batch = gen.create_batch(&schema, 8)?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(8, batch.num_rows());
        Ok(())
    }
}
