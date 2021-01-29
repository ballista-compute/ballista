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

use crate::error::{BallistaError, Result};
use crate::scheduler::SchedulerClient;
use crate::serde::scheduler::ExecutorMeta;

use async_trait::async_trait;
use uuid::Uuid;

pub struct StandaloneClient {}

impl StandaloneClient {
    pub fn new(
        _registrar_url: &str,
        _registrar_port: usize,
        _executor_uuid: &Uuid,
        _executor_host: &str,
        _executor_port: usize,
    ) -> Self {
        //TODO start thread that will periodically register this executor with the registrar
        // using protobuf messages and client.rs to send them

        Self {}
    }
}

#[async_trait]
impl SchedulerClient for StandaloneClient {
    async fn get_executors(&self) -> Result<Vec<ExecutorMeta>> {
        //TODO connect to registrar to get a list of executors in this cluster using
        // protobuf messages and client.rs to send them
        Err(BallistaError::NotImplemented(
            "SchedulerClient.get_executors".to_owned(),
        ))
    }
}
