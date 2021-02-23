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
