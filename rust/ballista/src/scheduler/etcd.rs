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

use std::time::Duration;

use crate::error::{ballista_error, Result};
use crate::scheduler::SchedulerClient;
use crate::serde::scheduler::ExecutorMeta;

use async_trait::async_trait;
use etcd_client::{Client, GetOptions, PutOptions};
use log::{debug, warn};

pub struct EtcdClient {
    etcd_urls: String,
    cluster_name: String,
}

impl EtcdClient {
    pub fn new(etcd_urls: String, cluster_name: String, executor_meta: ExecutorMeta) -> Self {
        // Start a thread that will register the executor with etcd periodically
        tokio::spawn(main_loop(
            etcd_urls.to_owned(),
            cluster_name.to_owned(),
            executor_meta,
        ));

        Self {
            etcd_urls,
            cluster_name,
        }
    }
}

async fn main_loop(etcd_urls: String, cluster_name: String, executor_meta: ExecutorMeta) {
    loop {
        match Client::connect([&etcd_urls], None).await {
            Ok(mut client) => {
                debug!("Connected to etcd at {} ok", etcd_urls);
                let lease_time_seconds = 60;
                let key = format!("/ballista/{}/{}", cluster_name, executor_meta.id);
                let value = format!("{}:{}", executor_meta.host, executor_meta.port);
                match client.lease_grant(lease_time_seconds, None).await {
                    Ok(lease) => {
                        let options = PutOptions::new().with_lease(lease.id());
                        match client.put(key.clone(), value.clone(), Some(options)).await {
                            Ok(_) => debug!("Registered with etcd as {}.", key),
                            Err(e) => warn!("etcd put failed: {:?}", e.to_string()),
                        }
                    }
                    Err(e) => warn!("etcd lease grant failed: {:?}", e.to_string()),
                }
            }
            Err(e) => warn!("Failed to connect to etcd {:?}", e.to_string()),
        }
        tokio::time::delay_for(Duration::from_secs(15)).await;
    }
}

#[async_trait]
impl SchedulerClient for EtcdClient {
    async fn get_executors(&self) -> Result<Vec<ExecutorMeta>> {
        match Client::connect([&self.etcd_urls], None).await {
            Ok(mut client) => {
                debug!("get_executor_ids got client");
                let key = format!("/ballista/{}", &self.cluster_name);
                let resp = client
                    .get(key, Some(GetOptions::new().with_all_keys()))
                    .await
                    .map_err(|e| ballista_error(&format!("etcd error {:?}", e)))?;

                let mut execs = vec![];
                for kv in resp.kvs() {
                    let executor_id = kv.key_str().expect("etcd - empty string in map key");
                    let host_port = kv.value_str().expect("etcd - empty string in map value");
                    let host_port: Vec<_> = host_port.split(':').collect();
                    if host_port.len() == 2 {
                        let host = &host_port[0];
                        let port = &host_port[1];
                        if let Ok(port) = port.to_string().parse::<u16>() {
                            execs.push(ExecutorMeta {
                                id: executor_id.to_owned(),
                                host: host.to_string(),
                                port,
                            });
                        }
                    }
                }
                Ok(execs)
            }
            Err(e) => Err(ballista_error(&format!(
                "Failed to connect to etcd {:?}",
                e.to_string()
            ))),
        }
    }
}
