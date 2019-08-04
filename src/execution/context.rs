use crate::error::Result;
use crate::execution::ExecutionPlan;
use std::thread;
use std::thread::JoinHandle;

/// Execution Context
#[derive(Default)]
pub struct Context {}

impl Context {
    /// Create a new execution context
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, plan: &dyn ExecutionPlan) -> Result<()> {
        // execute each partition on a thread
        use std::sync::atomic::{AtomicUsize, Ordering};
        let thread_id = AtomicUsize::new(1);
        let threads: Vec<JoinHandle<Result<()>>> = plan
            .partitions()?
            .iter()
            .map(|p| {
                let thread_id = thread_id.fetch_add(1, Ordering::SeqCst);
                let p = p.clone();
                thread::spawn(move || {
                    let it = p.execute().unwrap();
                    let mut it = it.lock().unwrap();
                    while let Ok(Some(batch)) = it.next() {
                        println!(
                            "thread {} got batch with {} rows",
                            thread_id,
                            batch.num_rows()
                        );
                    }
                    Ok(())
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap().unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::csv::CsvExec;
    use crate::execution::expressions::Column;
    use crate::execution::projection::ProjectionExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{BufReader, BufWriter};
    use std::path::Path;
    use std::sync::Arc;

    #[test]
    fn project_first_column() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c3", DataType::Int16, false),
            Field::new("c4", DataType::Int32, false),
            Field::new("c5", DataType::Int64, false),
            Field::new("c6", DataType::UInt8, false),
            Field::new("c7", DataType::UInt16, false),
            Field::new("c8", DataType::UInt32, false),
            Field::new("c9", DataType::UInt64, false),
            Field::new("c10", DataType::Float32, false),
            Field::new("c11", DataType::Float64, false),
            Field::new("c12", DataType::Utf8, false),
        ]));

        let partitions = 4;
        let path = create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::try_new(&path, schema, true, None, 1024)?;

        let projection = ProjectionExec::try_new(vec![Arc::new(Column::new(0))], Arc::new(csv))?;

        let context = Context::new();
        context.execute(&projection)?;

        //        let mut partition_count = 0;
        //        let mut row_count = 0;
        //        for partition in projection.partitions()? {
        //            partition_count += 1;
        //            let iterator = partition.execute()?;
        //            let mut iterator = iterator.lock().unwrap();
        //            while let Some(batch) = iterator.next()? {
        //                assert_eq!(1, batch.num_columns());
        //                row_count += batch.num_rows();
        //            }
        //        }
        //        assert_eq!(partitions, partition_count);
        //        assert_eq!(100, row_count);

        Ok(())
    }

    /// Generated partitioned copy of a CSV file
    fn create_partitioned_csv(filename: &str, partitions: usize) -> Result<String> {
        let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
        let path = format!("{}/csv/{}", testdata, filename);

        let mut dir = env::temp_dir();
        dir.push(&format!("{}-{}", filename, partitions));

        if Path::new(&dir).exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(dir.clone()).unwrap();

        let mut writers = vec![];
        for i in 0..partitions {
            let mut filename = dir.clone();
            filename.push(format!("part{}.csv", i));
            let writer = BufWriter::new(File::create(&filename).unwrap());
            writers.push(writer);
        }

        let f = File::open(&path)?;
        let f = BufReader::new(f);
        let mut i = 0;
        for line in f.lines() {
            let line = line.unwrap();

            if i == 0 {
                // write header to all partitions
                for w in writers.iter_mut() {
                    w.write(line.as_bytes()).unwrap();
                    w.write(b"\n").unwrap();
                }
            } else {
                // write data line to single partition
                let partition = i % partitions;
                writers[partition].write(line.as_bytes()).unwrap();
                writers[partition].write(b"\n").unwrap();
            }

            i += 1;
        }
        for w in writers.iter_mut() {
            w.flush().unwrap();
        }

        Ok(dir.as_os_str().to_str().unwrap().to_string())
    }
}
