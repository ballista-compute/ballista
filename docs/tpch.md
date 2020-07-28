# TPCH Overview

TPC-H is an industry standard benchmark for testing databases and query engines. A command-line tool is available that
can generate the raw test data at any given scale factor (scale factor refers to the amount of data to be generated).

The generated data is in CSV format (pipe-delimited text files to be more specific). It is necessary to convert these
files to Parquet before running benchmarks.

- Use [this repo](https://github.com/databricks/tpch-dbgen) to generate the text files.
- Spark code for converting to Parquet can be found [here](../spark/benchmarks)

For more information about TPC-H please refer to the [official website](http://www.tpc.org/tpch/).