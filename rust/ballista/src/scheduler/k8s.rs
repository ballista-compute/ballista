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

//! Ballista k8s cluster management utilities

/*
use crate::error::BallistaError;
use crate::scheduler::SchedulerClient;
use crate::serde::scheduler::ExecutorMeta;

use async_trait::async_trait;
use k8s_openapi::api;

const CLUSTER_LABEL_KEY: &str = "ballista-cluster";

pub struct KubernetesClient {
    namespace: String,
    cluster_name: String,
}

impl KubernetesClient {
    pub fn new(namespace: String, cluster_name: String) -> Self {
        Self {
            namespace,
            cluster_name,
        }
    }
}

#[async_trait]
impl SchedulerClient for KubernetesClient {
    /// Get a list of executor nodes in a cluster by listing pods in the stateful set.
    async fn get_executors(&self) -> Result<Vec<ExecutorMeta>, BallistaError> {
        use api::core::v1::Pod;

        let client = kube::client::Client::try_default().await?;
        let pods: kube::api::Api<Pod> = kube::api::Api::namespaced(client, &self.namespace);

        let mut executors = vec![];

        let pods = pods
            .list(
                &kube::api::ListParams::default()
                    .labels(&format!("{}={}", CLUSTER_LABEL_KEY, &self.cluster_name)),
            )
            .await?;

        for pod in &pods {
            if let Some(pod_meta) = pod.metadata.as_ref() {
                if let Some(pod_name) = pod_meta.name.as_ref() {
                    if let Some(pod_spec) = pod.spec.as_ref() {
                        if !pod_spec.containers.is_empty() {
                            let host =
                                format!("{}.{}.{}", pod_name, &self.cluster_name, &self.namespace);

                            if let Some(port) = pod_spec.containers[0].ports.as_ref() {
                                if !port.is_empty() {
                                    executors.push(ExecutorMeta {
                                        id: pod_name.to_owned(),
                                        host,
                                        port: port[0].container_port as u16,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(executors)
    }
}
*/
