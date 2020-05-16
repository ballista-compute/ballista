use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;
use crate::arrow::record_batch::RecordBatch;

/** Create formatted result set from record batches */
pub fn result_str(results: &[RecordBatch]) -> Vec<String> {
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
                        let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int16 => {
                        let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int64 => {
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt8 => {
                        let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt16 => {
                        let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt32 => {
                        let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt64 => {
                        let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float32 => {
                        let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        let s = array.value(row_index);

                        str.push_str(&format!("{:?}", s));
                    }
                    _ => str.push_str("???"),
                }
            }
            result.push(str);
        }
    }
    result
}
