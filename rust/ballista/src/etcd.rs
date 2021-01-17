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

use std::thread;
use std::time::Duration;

use crate::error::{ballista_error, Result};
use crate::serde::scheduler::ExecutorMeta;

use etcd_client::{Client, GetOptions, PutOptions};
use log::{debug, warn};
use uuid::Uuid;

/// Start a thread that will register the executor with etcd periodically
pub fn start_etcd_thread(
    etcd_urls: &str,
    cluster_name: &str,
    uuid: &Uuid,
    host: &str,
    port: usize,
) {
    let etcd_urls = etcd_urls.to_owned();
    let cluster_name = cluster_name.to_owned();
    let uuid = uuid.to_owned();
    let host = host.to_owned();

    tokio::spawn(async move {
        main_loop(&etcd_urls, &cluster_name, &uuid, &host, port).await;
    });
}

async fn main_loop(etcd_urls: &str, cluster_name: &str, uuid: &Uuid, host: &str, port: usize) {
    loop {
        match Client::connect([&etcd_urls], None).await {
            Ok(mut client) => {
                debug!("Connected to etcd at {} ok", etcd_urls);
                let lease_time_seconds = 60;
                let key = format!("/ballista/{}/{}", cluster_name, &uuid);
                let value = format!("{}:{}", host, port);
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
        thread::sleep(Duration::from_secs(15));
    }
}

pub async fn etcd_get_executors(etcd_urls: &str, cluster_name: &str) -> Result<Vec<ExecutorMeta>> {
    match Client::connect([etcd_urls], None).await {
        Ok(mut client) => {
            debug!("get_executor_ids got client");
            let key = format!("/ballista/{}", cluster_name);
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
                    if let Ok(port) = port.to_string().parse::<usize>() {
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
