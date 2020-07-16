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

//! Relational operators that can be used in query plans. Relational operators represent concepts
//! such as projection, selection, aggregate, and join, and transform streams of data.

pub use csv_scan::CsvScanExec;
pub use filter::FilterExec;
pub use hash_aggregate::HashAggregateExec;
pub use in_memory::InMemoryTableScanExec;
pub use parquet_scan::ParquetScanExec;
pub use projection::ProjectionExec;
pub use shuffle_exchange::ShuffleExchangeExec;
pub use shuffle_reader::ShuffleReaderExec;

mod csv_scan;
mod filter;
mod hash_aggregate;
mod in_memory;
mod parquet_scan;
mod projection;
mod shuffle_exchange;
mod shuffle_reader;
