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

//! Utilities for printing columnar batches

use crate::arrow::array;
use crate::arrow::datatypes::DataType;
use crate::arrow::record_batch::RecordBatch;
use crate::cast_array;
use crate::error::{ballista_error, Result};

/// Create formatted result set from record batches
pub fn result_str(results: &[RecordBatch]) -> Result<Vec<String>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let mut str = String::new();
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    str.push_str("\t");
                }
                let column = batch.column(column_index);

                match column.data_type() {
                    DataType::Int8 => {
                        let array = cast_array!(column, Int8Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int16 => {
                        let array = cast_array!(column, Int16Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int32 => {
                        let array = cast_array!(column, Int32Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int64 => {
                        let array = cast_array!(column, Int64Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt8 => {
                        let array = cast_array!(column, UInt8Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt16 => {
                        let array = cast_array!(column, UInt16Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt32 => {
                        let array = cast_array!(column, UInt32Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt64 => {
                        let array = cast_array!(column, UInt64Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float32 => {
                        let array = cast_array!(column, Float32Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float64 => {
                        let array = cast_array!(column, Float64Array)?;
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Utf8 => {
                        let array = cast_array!(column, StringArray)?;
                        let s = array.value(row_index);

                        str.push_str(&format!("{:?}", s));
                    }
                    _ => str.push_str("???"),
                }
            }
            result.push(str);
        }
    }
    Ok(result)
}
