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


#[cxx::bridge]
pub mod ffi {

    struct Aggregate {
        /// Aggregate expression to perform e.g. "SUM"
        aggr_expr: String,
        /// Index of the column to perform the aggregate on
        value_index: u32,
    }

    extern "C" {
        include!("c/library.hpp");

        type Table;
        type TableView;

        pub fn read_parquet(filename: &str) -> UniquePtr<Table>;
        pub fn write_parquet(filename: &str, table: UniquePtr<Table>);

        pub fn aggregate(
            table: UniquePtr<Table>,
            keys: Vec<u32>,
            aggr: Vec<Aggregate>,
        ) -> UniquePtr<Table>;
    }
}

#[cfg(test)]
mod tests {
    use super::ffi::*;

    #[test]
    unsafe fn test_parquet_roundtrip() {
        let table = read_parquet(filename());
        write_parquet("/tmp/test_parquet_roundtrip.parquet", table);
    }

    #[test]
    unsafe fn test_aggregate() {
        let table = read_parquet(filename());
        let group_by = vec![0];
        let aggregates = vec![Aggregate {
            aggr_expr: "SUM".to_string(),
            value_index: 1,
        }];
        let aggr_result = aggregate(table, group_by, aggregates);
        write_parquet("/tmp/test_aggregate.parquet", aggr_result);
    }

    fn filename() -> &'static str {
        "testdata/example1.snappy.parquet"
    }
}
