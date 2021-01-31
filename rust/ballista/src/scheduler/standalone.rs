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

use crate::error::{ballista_error, Result};
use crate::scheduler::ConfigBackendClient;

use log::warn;

/// A [`ConfigBackendClient`] implementation that uses file-based storage to save cluster configuration.
#[derive(Clone)]
pub struct StandaloneClient {
    db: sled::Db,
}

impl StandaloneClient {
    /// Creates a StandaloneClient that saves data to the specified file.
    pub fn try_new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Ok(Self { db: sled::open(path)? })
    }

    /// Creates a StandaloneClient that saves data to a temp file.
    pub fn try_new_temporary() -> Result<Self> {
        Ok(Self {
            db: sled::Config::new().temporary(true).open()?,
        })
    }
}

#[tonic::async_trait]
impl ConfigBackendClient for StandaloneClient {
    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .db
            .get(key)
            .map_err(|e| ballista_error(&format!("sled error {:?}", e)))?
            .map(|v| v.to_vec())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&mut self, prefix: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .db
            .scan_prefix(prefix)
            .map(|v| v.map(|(_key, value)| value.to_vec()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ballista_error(&format!("sled error {:?}", e)))?)
    }

    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<()> {
        self.db
            .insert(key, value)
            .map_err(|e| {
                warn!("sled insert failed: {}", e);
                ballista_error("sled insert failed")
            })
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::ConfigBackendClient;

    use super::StandaloneClient;
    use std::result::Result;

    fn create_instance() -> Result<StandaloneClient, Box<dyn std::error::Error>> {
        Ok(StandaloneClient::try_new_temporary()?)
    }

    #[tokio::test]
    async fn put_read() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        client.put(key.to_owned(), value.to_vec()).await?;
        assert_eq!(client.get(key).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn read_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = create_instance()?;
        let key = "key";
        let empty: &[u8] = &[];
        assert_eq!(client.get(key).await?, empty);
        Ok(())
    }

    #[tokio::test]
    async fn read_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        client.put(format!("{}/1", key), value.to_vec()).await?;
        client.put(format!("{}/2", key), value.to_vec()).await?;
        assert_eq!(client.get_from_prefix(key).await?, vec![value, value]);
        Ok(())
    }
}
