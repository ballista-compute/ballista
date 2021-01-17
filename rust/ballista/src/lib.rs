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

//! Ballista Distributed Compute

pub const BALLISTA_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod client;
pub mod columnar_batch;
pub mod context;
pub mod error;
pub mod etcd;
pub mod executor;
pub mod flight_service;
pub mod k8s;
pub mod memory_stream;
pub mod prelude;
pub mod shuffle_write;
pub mod utils;

#[macro_use]
pub mod serde;
