extern crate cudf_rs;

use cudf_rs::ffi::*;

pub fn main() {
    let filename = "testdata/example1.snappy.parquet";
    let table = read_parquet(filename);
    let group_by = vec![0];
    let aggregates = vec![Aggregate {
        aggr_expr: "SUM".to_string(),
        value_index: 1,
    }];
    let result = aggregate(table, group_by, aggregates);
    write_parquet("aggregate-result.parquet", result);
}
