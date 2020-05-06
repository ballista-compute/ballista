//! Ballista cluster management utilities

use k8s_openapi::{
    api,
    //    apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta},
    http,
    Response,
    ResponseBody,
};
use kube::config;

use crate::error::BallistaError;

const CLUSTER_LABEL_KEY: &str = "ballista-cluster";

fn execute<T, F>(request: http::Request<Vec<u8>>, response_body: F) -> Result<T, BallistaError>
where
    T: Response,
    F: Fn(http::StatusCode) -> ResponseBody<T>,
{
    let kubeconfig = config::load_kube_config()
        .or_else(|_| config::incluster_config())
        .map_err(|_| "Failed to load kubeconfig".to_string())?;
    let client = kubeconfig.client;

    let (parts, body) = request.into_parts();
    let uri_str = format!("{}{}", kubeconfig.base_path, parts.uri);
    let req = match parts.method {
        http::Method::GET => client.get(&uri_str),
        http::Method::POST => client.post(&uri_str),
        http::Method::DELETE => client.delete(&uri_str),
        http::Method::PUT => client.put(&uri_str),
        other => {
            return Err(BallistaError::General(format!("Invalid method: {}", other)));
        }
    }
    .body(body);

    let mut response = req.send()?;
    let status = response.status();
    let mut r = response_body(status);
    r.append_slice(response.text()?.as_bytes());
    Ok(r.parse()?)
}

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
pub fn get_executors(cluster_name: &str, namespace: &str) -> Result<Vec<Executor>, BallistaError> {
    use api::core::v1::{ListNamespacedPodOptional, ListNamespacedPodResponse, Pod};

    let (request, response_body) = Pod::list_namespaced_pod(
        namespace,
        ListNamespacedPodOptional {
            label_selector: Some(&format!("{}={}", CLUSTER_LABEL_KEY, cluster_name)),
            ..Default::default()
        },
    )?;
    let response = execute(request, response_body)?;

    match response {
        ListNamespacedPodResponse::Ok(pod_list) => Ok(pod_list
            .items
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
            .collect()),
        other => Err(BallistaError::General(format!(
            "Unexpected response from Kubernetes API: {:?}",
            other
        ))),
    }
}
