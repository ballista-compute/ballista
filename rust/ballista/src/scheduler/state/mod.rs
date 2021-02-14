use std::{
    any::type_name,
    io::{Cursor, Read},
    time::Duration,
};

use log::debug;
use prost::Message;

use crate::error::Result;
use crate::serde::protobuf::{ExecutorMetadata, JobStatus};
use crate::{error::ballista_error, prelude::BallistaError, serde::scheduler::ExecutorMeta};

use super::SchedulerServer;

mod etcd;
mod standalone;

pub use etcd::EtcdClient;
pub use standalone::StandaloneClient;

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
        let mut result = vec![];

        let entries = self
            .config_client
            .clone()
            .get_from_prefix(&get_executors_prefix(namespace))
            .await?;
        for entry in entries {
            let meta: ExecutorMetadata = decode_protobuf(&entry)?;
            result.push(meta.into());
        }
        Ok(result)
    }

    pub async fn save_executor_metadata(&self, namespace: &str, meta: ExecutorMeta) -> Result<()> {
        let key = get_executor_key(namespace, &meta.id);
        let meta: ExecutorMetadata = meta.into();
        let value: Vec<u8> = encode_protobuf(&meta)?;
        self.config_client
            .clone()
            .put(key, value, Some(LEASE_TIME))
            .await
    }

    pub async fn save_job_metadata(
        &self,
        namespace: &str,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<()> {
        debug!("Saving job metadata: {:?}", status);
        let key = get_job_key(namespace, job_id);
        let value = encode_protobuf(status)?;
        self.config_client.clone().put(key, value, None).await
    }

    pub async fn get_job_metadata(&self, namespace: &str, job_id: &str) -> Result<JobStatus> {
        let key = get_job_key(namespace, job_id);
        let value = &self.config_client.clone().get(&key).await?;
        let value: JobStatus = decode_protobuf(value)?;
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

fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!("Could not deserialize {}: {}", type_name::<T>(), e))
    })
}

fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!("Could not serialize {}: {}", type_name::<T>(), e))
    })?;
    Ok(value)
}
