use std::time::Duration;

use log::debug;

use crate::{error::ballista_error, prelude::BallistaError, serde::scheduler::ExecutorMeta};
use crate::{error::Result, serde::scheduler::JobMeta};

use super::SchedulerServer;

pub mod etcd;
pub mod standalone;

const LEASE_TIME: Duration = Duration::from_secs(60);

/// A trait that contains the necessary methods to save and retrieve the state and configuration of a cluster.
#[tonic::async_trait]
pub trait ConfigBackendClient: Clone {
    /// Retrieve the data associated with a specific key.
    ///
    /// An empty vec is returned if the key does not exist.
    async fn get(&mut self, key: &str) -> Result<Vec<u8>>;

    /// Retrieve all data associated with a specific key.
    async fn get_from_prefix(&mut self, prefix: &str) -> Result<Vec<Vec<u8>>>;

    /// Saves the value into the provided key, overriding any previous data that might have been associated to that key.
    async fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
        lease_time: Option<Duration>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub(super) struct SchedulerState<Config: ConfigBackendClient> {
    config_client: Config,
}

impl<Config: ConfigBackendClient> SchedulerState<Config> {
    pub fn new(config_client: Config) -> Self {
        Self { config_client }
    }

    pub async fn get_executors_metadata(&self, namespace: &str) -> Result<Vec<ExecutorMeta>> {
        self.config_client
            .clone()
            .get_from_prefix(&get_executors_prefix(namespace))
            .await?
            .into_iter()
            .map(|bytes| serde_json::from_slice::<ExecutorMeta>(&bytes))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                BallistaError::Internal(format!("Could not deserialize state value: {}", e))
            })
    }

    pub async fn save_executor_metadata(&self, namespace: &str, meta: &ExecutorMeta) -> Result<()> {
        let key = get_executor_key(namespace, &meta.id);
        let value = serde_json::to_vec(meta).map_err(|e| {
            BallistaError::Internal(format!("Could not serialize ExecutorMeta: {}", e))
        })?;
        self.config_client
            .clone()
            .put(key, value, Some(LEASE_TIME))
            .await
    }

    pub async fn save_job_metadata(&self, namespace: &str, meta: &JobMeta) -> Result<()> {
        debug!("Saving job metadata: {:?}", meta);
        let key = get_job_key(namespace, &meta.id);
        let value = serde_json::to_vec(meta).map_err(|e| {
            BallistaError::Internal(format!("Could not serialize ExecutorMeta: {}", e))
        })?;
        self.config_client.clone().put(key, value, None).await
    }

    pub async fn get_job_metadata(&self, namespace: &str, job_id: &str) -> Result<JobMeta> {
        let key = get_job_key(namespace, job_id);
        let value = &self.config_client.clone().get(&key).await?;
        let msg = String::from_utf8(value.clone()).unwrap();
        debug!("Going to deserialize {}", msg);
        let value: JobMeta = serde_json::from_slice(value).map_err(|e| {
            BallistaError::Internal(format!("Could not deserialize state value: {}", e))
        })?;
        Ok(value)
    }
}

fn get_executors_prefix(namespace: &str) -> String {
    format!("/ballista/executors/{}", namespace)
}

fn get_executor_key(namespace: &str, id: &str) -> String {
    format!("{}/{}", get_executors_prefix(namespace), id)
}

fn get_job_key(namespace: &str, id: &str) -> String {
    format!("/ballista/jobs/{}/{}", namespace, id)
}
