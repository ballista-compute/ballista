# Experimental Rust bindings for cuDF

This is not an official NVIDIA project or in any way endorsed or supported by NVIDIA. I am just curious to see if I can 
build a proof-of-concept of Rust bindings for [cuDF](https://github.com/rapidsai/cudf).

The approach I am taking is to use [https://github.com/dtolnay/cxx](https://github.com/dtolnay/cxx) to generate the 
bindings since bindgen doesn't support C++ well (I did try this approach first).

```rust
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
```

## Status

- [x] Roundtrip read & write a Parquet file
- [x] Perform an aggregate
- [ ] Error handling
- [ ] Implement transferring a table_view to host memory so results can be retrieved
- [ ] Wrap in Rust DataFrame API from Apache Arrow / DataFusion for better UX
- [ ] Add DataFrame and SQL examples
- [ ] Add support for more operations

## Pre-requisites

- Install CUDA 10.2
- Install cuDF 0.15

## Compiling

The cargo build generates necessary bindings and compiles the C++ and the Rust code.

```bash
cargo test
```


 


