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

//! Support for etcd discovery mechanism.

use crate::error::{ballista_error, Result};
use crate::scheduler::ConfigBackendClient;

use etcd_client::{GetOptions, PutOptions};
use log::warn;

#[derive(Clone)]
pub struct EtcdClient {
    etcd: etcd_client::Client,
}

impl EtcdClient {
    pub fn new(etcd: etcd_client::Client) -> Self {
        Self { etcd }
    }
}

#[tonic::async_trait]
impl ConfigBackendClient for EtcdClient {
    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .etcd
            .get(key, None)
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .get(0)
            .map(|kv| kv.value().to_owned())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&mut self, prefix: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .etcd
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?
            .kvs()
            .iter()
            .map(|kv| kv.value().to_owned())
            .collect())
    }

    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        let lease_time_seconds = 60;
        match self.etcd.lease_grant(lease_time_seconds, None).await {
            Ok(lease) => {
                let options = PutOptions::new().with_lease(lease.id());
                self.etcd
                    .put(key.clone(), value.clone(), Some(options))
                    .await
                    .map_err(|e| {
                        warn!("etcd put failed: {}", e);
                        ballista_error("etcd put failed")
                    })
                    .map(|_| ())
            }
            Err(e) => {
                warn!("etcd lease grant failed: {:?}", e.to_string());
                Err(ballista_error("etcd lease grant failed"))
            }
        }
    }
}
