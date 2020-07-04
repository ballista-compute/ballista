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

//! Experimental distributed query execution support.

pub mod filter;
pub mod hash_aggregate;
pub mod parquet_scan;
pub mod physical_plan;
pub mod projection;
pub mod scheduler;
pub mod shuffle_exchange;
pub mod shuffle_reader;
pub mod shuffled_hash_join;
