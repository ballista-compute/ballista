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

//! Support for distributed schedulers, such as Kubernetes

pub mod etcd;
pub mod k8s;
pub mod standalone;

use crate::error::Result;
use crate::serde::scheduler::ExecutorMeta;

use async_trait::async_trait;

/// Client API that an executor can use to interact with the cluster
#[async_trait]
pub trait SchedulerClient: Sync + Send {
    /// Get a list of executors in the cluster
    async fn get_executors(&self) -> Result<Vec<ExecutorMeta>>;
}
