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

//! Ballista cluster management utilities

use k8s_openapi::api;
use kube;

use crate::error::BallistaError;

const CLUSTER_LABEL_KEY: &str = "ballista-cluster";

/// Ballista executor
pub struct Executor {
    pub host: String,
    pub port: usize,
}

impl Executor {
    pub fn new(host: &str, port: usize) -> Self {
        Self {
            host: host.to_owned(),
            port,
        }
    }
}

/// Get a list of executor nodes in a cluster by listing pods in the stateful set.
pub async fn get_executors(
    cluster_name: &str,
    namespace: &str,
) -> Result<Vec<Executor>, BallistaError> {
    use api::core::v1::Pod;

    let client = kube::client::Client::try_default().await?;
    let pods: kube::api::Api<Pod> = kube::api::Api::namespaced(client, namespace);

    let executors = pods
        .list(
            &kube::api::ListParams::default()
                .labels(&format!("{}={}", CLUSTER_LABEL_KEY, cluster_name)),
        )
        .await?
        .iter()
        .map(|pod| {
            let pod_meta = pod.metadata.as_ref().unwrap();
            Executor {
                host: format!(
                    "{}.{}.{}",
                    pod_meta.name.as_ref().unwrap().clone(),
                    cluster_name,
                    namespace,
                ),
                port: pod.spec.as_ref().unwrap().containers[0]
                    .ports
                    .as_ref()
                    .unwrap()[0]
                    .container_port as usize,
            }
        })
        .collect();

    Ok(executors)
}
