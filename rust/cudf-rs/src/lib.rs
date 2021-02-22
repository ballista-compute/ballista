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
    fn test_parquet_roundtrip() {
        let table = read_parquet(filename());
        write_parquet("/tmp/test_parquet_roundtrip.parquet", table);
    }

    #[test]
    fn test_aggregate() {
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
