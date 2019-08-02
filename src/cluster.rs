//! Ballista cluster management utilities

use std::{collections::BTreeMap, convert::TryInto};

use k8s_openapi::{
    api,
    apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta},
    http, Response, ResponseBody,
};
use kube::config;

use crate::error::BallistaError;

const CLUSTER_LABEL_KEY: &str = "ballista-cluster";
const DEFAULT_IMAGE: &str = "andygrove/ballista:0.1.3";

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

pub fn statefulset_name(name: &str) -> String {
    format!("ballista-{}", name)
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
    if let ListNamespacedPodResponse::Ok(pod_list) = response {
        Ok(pod_list
            .items
            .iter()
            .map(|pod| {
                let pod_meta = pod.metadata.as_ref().unwrap();
                Executor {
                    host: format!(
                        "{}.{}.{}",
                        pod_meta.name.as_ref().unwrap().clone(),
                        statefulset_name(cluster_name),
                        namespace,
                    ),
                    port: pod.spec.as_ref().unwrap().containers[0]
                        .ports
                        .as_ref()
                        .unwrap()[0]
                        .container_port as usize,
                }
            })
            .collect())
    } else {
        Err(BallistaError::General(
            "Unexpected response from Kubernetes API".to_string(),
        ))
    }
}

macro_rules! check_create_response {
    ($response:ident, $rtype:ident) => {{
        let out: Result<(), BallistaError> = match $response {
            // Successful response
            Ok($rtype::Ok(_)) | Ok($rtype::Created(_)) | Ok($rtype::Accepted(_)) => Ok(()),

            // Some unexpected response
            // (not HTTP 200, but still parsed successfully)
            Ok(other) => Err(format!("Unexpected response: {:?}", other).into()),

            // Need more response data.
            // Read more bytes from the response into the `ResponseBody`
            Err(BallistaError::KubeAPIResponseError(k8s_openapi::ResponseError::NeedMoreData)) => {
                Err("Need more response data".to_string().into())
            }

            // Some other error, like the response body being
            // malformed JSON or invalid UTF-8.
            Err(err) => Err(format!("error: {:?}", err).into()),
        };
        out
    }};
}

macro_rules! check_delete_response {
    ($response:ident, $rtype:ident) => {{
        let out: Result<(), BallistaError> = match $response {
            // Successful response
            Ok($rtype::OkStatus(_)) | Ok($rtype::OkValue(_)) | Ok($rtype::Accepted(_)) => Ok(()),

            // Some unexpected response
            // (not HTTP 200, but still parsed successfully)
            Ok(other) => Err(format!("Unexpected response: {:?}", other).into()),

            // Need more response data.
            // Read more bytes from the response into the `ResponseBody`
            Err(BallistaError::KubeAPIResponseError(k8s_openapi::ResponseError::NeedMoreData)) => {
                Err("Need more response data".to_string().into())
            }

            // Some other error, like the response body being
            // malformed JSON or invalid UTF-8.
            Err(err) => Err(format!("error: {:?}", err).into()),
        };
        out
    }};
}

/// A Volume to be attached to a cluster.
#[derive(Debug)]
pub struct Volume {
    /// The name of the volume.
    ///
    /// For now, this must match the name of an existing Persistent Volume
    /// in the Kubernetes cluster. It will be used as the name for a
    /// Persistent Volume Claim and for the Volume in the executor pods.
    name: String,
    /// The path to mount the volume in the executor pods.
    mount_path: String,
}

impl std::str::FromStr for Volume {
    type Err = BallistaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.splitn(2, ":");
        let name = split
            .next()
            .ok_or_else(|| BallistaError::General(format!(
                "Invalid volume spec '{}'. Volumes should be in the format '<volume name>:<mount path>'.",
                s)))?
            .to_string();
        let mount_path = split
            .next()
            .ok_or_else(|| BallistaError::General(format!(
                "Invalid volume spec '{}'. Volumes should be in the format '<volume name>:<mount path>'.",
                s)))?
            .to_string();
        Ok(Volume { name, mount_path })
    }
}

#[derive(Debug)]
pub struct ClusterBuilder {
    /// The name of the executor cluster.
    name: String,
    /// The Kubernetes namespace in which the executor should live.
    namespace: String,
    /// The number of replicas to use in the executor.
    replicas: usize,
    /// The image to use.
    image: Option<String>,
    /// Volumes to mount into the executors.
    volumes: Option<Vec<Volume>>,
}

impl ClusterBuilder {
    /// Builder for creating a new executor.
    pub fn new(name: String, namespace: String, replicas: usize) -> Self {
        ClusterBuilder {
            name,
            namespace,
            replicas,
            image: None,
            volumes: None,
        }
    }

    /// Customise the image used for the executors.
    pub fn image(&mut self, image: Option<String>) -> &mut Self {
        self.image = image;
        self
    }

    /// Create a persistent volume claim and mount into the executors.
    ///
    /// The name of the volume is assumed to match the name of an
    /// existing Persistent Volume in Kubernetes.
    pub fn volumes(&mut self, volumes: Option<Vec<Volume>>) -> &mut Self {
        self.volumes = volumes;
        self
    }

    /// Create the BTreeMap of labels to use in the Service and StatefulSet.
    ///
    /// Note that these are currently used for the Service and StatefulSet
    /// selectors, so it map not be appropriate to add too many restrictive
    /// labels here.
    fn get_labels(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(CLUSTER_LABEL_KEY.to_string(), self.name.clone());
        labels
    }

    fn create_service(
        &self,
        name: String,
        labels: BTreeMap<String, String>,
    ) -> Result<(), BallistaError> {
        use api::core::v1::{CreateNamespacedServiceResponse, Service, ServicePort, ServiceSpec};
        let service = Service {
            metadata: Some(ObjectMeta {
                name: Some(name),
                ..Default::default()
            }),
            spec: Some(ServiceSpec {
                cluster_ip: None,
                selector: Some(labels),
                ports: Some(vec![ServicePort {
                    port: 9090,
                    name: Some("grpc".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (request, response_body) =
            Service::create_namespaced_service(&self.namespace, &service, Default::default())?;
        // Actually create the service in K8S.
        let response = execute(request, response_body);
        check_create_response!(response, CreateNamespacedServiceResponse)?;
        Ok(())
    }

    fn create_persistent_volume_claim(
        &self,
        pvc_name: String,
        labels: BTreeMap<String, String>,
    ) -> Result<(), BallistaError> {
        use api::core::v1::{
            CreateNamespacedPersistentVolumeClaimResponse, PersistentVolumeClaim,
            PersistentVolumeClaimSpec, ResourceRequirements,
        };
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        let pvc = PersistentVolumeClaim {
            metadata: Some(ObjectMeta {
                name: Some(pvc_name.clone()),
                labels: Some(labels),
                ..Default::default()
            }),
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadOnlyMany".to_string()]),
                resources: Some(ResourceRequirements {
                    requests: {
                        let mut requests = BTreeMap::new();
                        requests.insert("storage".to_string(), Quantity("5Gi".to_string()));
                        Some(requests)
                    },
                    ..Default::default()
                }),
                storage_class_name: Some("".to_string()),
                volume_name: Some(pvc_name),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (request, response_body) =
            PersistentVolumeClaim::create_namespaced_persistent_volume_claim(
                &self.namespace,
                &pvc,
                Default::default(),
            )?;
        // Actually create the service in K8S.
        let response = execute(request, response_body);
        check_create_response!(response, CreateNamespacedPersistentVolumeClaimResponse)?;
        Ok(())
    }

    fn create_stateful_set(
        &self,
        name: String,
        labels: BTreeMap<String, String>,
    ) -> Result<(), BallistaError> {
        use api::{
            apps::v1::{CreateNamespacedStatefulSetResponse, StatefulSet, StatefulSetSpec},
            core::v1::{
                Container, ContainerPort, PersistentVolumeClaimVolumeSource, PodSpec,
                PodTemplateSpec, Volume, VolumeMount,
            },
        };
        // Now create the StatefulSet to manage some pods with persistent identifiers.
        let stateful_set = StatefulSet {
            metadata: Some(ObjectMeta {
                name: Some(name.clone()),
                ..Default::default()
            }),
            spec: Some(StatefulSetSpec {
                replicas: Some(self.replicas.try_into().unwrap_or(std::i32::MAX)),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    ..Default::default()
                },
                service_name: name,
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: "executor".to_string(),
                            image: Some(
                                self.image
                                    .clone()
                                    .unwrap_or_else(|| DEFAULT_IMAGE.to_string()),
                            ),
                            ports: Some(vec![ContainerPort {
                                container_port: 9090,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            }]),
                            volume_mounts: self.volumes.as_ref().map(|vs| {
                                vs.clone()
                                    .into_iter()
                                    .map(|v| VolumeMount {
                                        name: v.name.clone(),
                                        mount_path: v.mount_path.clone(),
                                        ..Default::default()
                                    })
                                    .collect()
                            }),
                            ..Default::default()
                        }],
                        volumes: self.volumes.as_ref().map(|vs| {
                            vs.clone()
                                .into_iter()
                                .map(|v| Volume {
                                    name: v.name.clone(),
                                    persistent_volume_claim: Some(
                                        PersistentVolumeClaimVolumeSource {
                                            claim_name: v.name.clone(),
                                            ..Default::default()
                                        },
                                    ),
                                    ..Default::default()
                                })
                                .collect()
                        }),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let (request, response_body) = StatefulSet::create_namespaced_stateful_set(
            &self.namespace,
            &stateful_set,
            Default::default(),
        )?;
        // Actually create the service in K8S.
        let response = execute(request, response_body);
        check_create_response!(response, CreateNamespacedStatefulSetResponse)?;
        Ok(())
    }

    /// Create the executor on Kubernetes.
    ///
    /// TODO: add config argument to allow alternative Kubernetes
    /// configuration specs (contexts etc). This would be passed through
    /// to `execute`.
    pub fn create(&self) -> Result<(), BallistaError> {
        let name = statefulset_name(&self.name);
        // Use these labels for both the Service selector and StatefulSet.
        let labels = self.get_labels();
        if let Some(vs) = self.volumes.as_ref() {
            for volume in vs {
                self.create_persistent_volume_claim(volume.name.clone(), labels.clone())?;
            }
        }
        self.create_service(name.clone(), labels.clone())?;
        self.create_stateful_set(name.clone(), labels.clone())?;
        Ok(())
    }
}

/// Create a Ballista application job in Kubernetes
pub fn create_ballista_application(
    namespace: &str,
    name: String,
    image_name: String,
) -> Result<(), BallistaError> {
    use api::{
        batch::v1::{CreateNamespacedJobResponse, Job, JobSpec},
        core::v1::{Container, PodSpec, PodTemplateSpec},
    };

    let job = Job {
        metadata: Some(ObjectMeta {
            name: Some(name.clone()),
            ..Default::default()
        }),
        spec: Some(JobSpec {
            backoff_limit: Some(1),
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name,
                        image: Some(image_name),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        status: Default::default(),
    };
    let (request, response_body) = Job::create_namespaced_job(namespace, &job, Default::default())?;
    let response = execute(request, response_body);
    check_create_response!(response, CreateNamespacedJobResponse)?;

    Ok(())
}

/// Delete a Ballista cluster.
pub fn delete_cluster(cluster_name: &str, namespace: &str) -> Result<(), BallistaError> {
    use api::{
        apps::v1::{DeleteNamespacedStatefulSetResponse, StatefulSet},
        core::v1::{
            DeleteNamespacedPersistentVolumeClaimResponse, DeleteNamespacedServiceResponse,
            ListNamespacedPersistentVolumeClaimOptional,
            ListNamespacedPersistentVolumeClaimResponse, PersistentVolumeClaim, Service,
        },
    };

    let statefulset_name = statefulset_name(cluster_name);

    // First delete the StatefulSet.
    let (request, response_body) = StatefulSet::delete_namespaced_stateful_set(
        &statefulset_name,
        namespace,
        Default::default(),
    )?;
    let response = execute(request, response_body);
    match check_delete_response!(response, DeleteNamespacedStatefulSetResponse) {
        Ok(_) => {}
        Err(e) => {
            println!("Error deleting StatefulSet: {:?}", e);
        }
    };

    // Then delete the Service.
    let (request, response_body) =
        Service::delete_namespaced_service(&statefulset_name, namespace, Default::default())?;
    let response = execute(request, response_body);
    match check_delete_response!(response, DeleteNamespacedServiceResponse) {
        Ok(_) => {}
        Err(e) => {
            println!("Error deleting Service: {:?}", e);
        }
    };

    // Finally, delete any PVCs associated with the cluster.
    let (request, response_body) = PersistentVolumeClaim::list_namespaced_persistent_volume_claim(
        namespace,
        ListNamespacedPersistentVolumeClaimOptional {
            label_selector: Some(&format!("{}={}", CLUSTER_LABEL_KEY, cluster_name)),
            ..Default::default()
        },
    )?;
    let response = execute(request, response_body)?;
    if let ListNamespacedPersistentVolumeClaimResponse::Ok(list) = response {
        for pvc in list.items {
            let (request, response_body) =
                PersistentVolumeClaim::delete_namespaced_persistent_volume_claim(
                    pvc.metadata.as_ref().unwrap().name.as_ref().unwrap(),
                    namespace,
                    Default::default(),
                )?;
            let response = execute(request, response_body);
            check_delete_response!(response, DeleteNamespacedPersistentVolumeClaimResponse)?
        }
    }
    Ok(())
}
