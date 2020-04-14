
use arrow::datatypes::Schema;
use crate::dataframe::DataFrame;
use crate::error::Result;

pub struct Context {

}

impl Context {

    pub fn read_csv(path: &str, schema: Option<Schema>, has_header: bool) -> Result<Box<dyn DataFrame>> {
        unimplemented!()
    }

    pub fn read_parquet(path: &str) -> Result<Box<dyn DataFrame>> {
        unimplemented!()
    }

}