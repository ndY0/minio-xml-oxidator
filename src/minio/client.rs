//! MinIO client pool keyed by configuration name.

use std::collections::HashMap;

use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::MinioClient;

use crate::config::MinioConfig;

/// Holds one `MinioClient` per configured MinIO instance, keyed by name.
pub struct MinioClientPool {
    clients: HashMap<String, MinioClient>,
}

impl MinioClientPool {
    /// Builds a client for each `MinioConfig`, returning an error if any
    /// endpoint URL is invalid or client construction fails.
    pub fn from_configs(configs: &[MinioConfig]) -> anyhow::Result<Self> {
        let mut clients = HashMap::new();
        for cfg in configs {
            let base_url: BaseUrl = cfg.endpoint.parse().map_err(|e| {
                anyhow::anyhow!(
                    "invalid endpoint '{}' for minio config '{}': {}",
                    cfg.endpoint,
                    cfg.name,
                    e
                )
            })?;
            let provider = StaticProvider::new(&cfg.access_key, &cfg.secret_key, None);
            let client =
                MinioClient::new(base_url, Some(provider), None, cfg.secure.map(|s| !s))
                    .map_err(|e| {
                        anyhow::anyhow!("failed to create minio client '{}': {}", cfg.name, e)
                    })?;
            clients.insert(cfg.name.clone(), client);
        }
        Ok(Self { clients })
    }

    /// Returns the client registered under `name`, or `None`.
    pub fn get(&self, name: &str) -> Option<&MinioClient> {
        self.clients.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_configs_produces_empty_pool() {
        let pool = MinioClientPool::from_configs(&[]).unwrap();
        assert!(pool.get("anything").is_none());
    }

    #[test]
    fn invalid_endpoint_returns_error() {
        let configs = vec![MinioConfig {
            name: "bad".to_owned(),
            endpoint: "not a url".to_owned(),
            access_key: "a".to_owned(),
            secret_key: "s".to_owned(),
            region: None,
            secure: None,
        }];
        let result = MinioClientPool::from_configs(&configs);
        assert!(result.is_err());
    }

    #[test]
    fn valid_config_creates_pool() {
        let configs = vec![MinioConfig {
            name: "local".to_owned(),
            endpoint: "http://localhost:9000".to_owned(),
            access_key: "admin".to_owned(),
            secret_key: "secret".to_owned(),
            region: None,
            secure: Some(false),
        }];
        let pool = MinioClientPool::from_configs(&configs).unwrap();
        assert!(pool.get("local").is_some());
        assert!(pool.get("other").is_none());
    }
}
