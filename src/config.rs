//! TOML configuration parsing for server, pipeline, and MinIO settings.

use serde::Deserialize;

/// Top-level application configuration loaded from a TOML file.
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    /// gRPC server settings.
    pub server: ServerConfig,
    /// Optional xml-oxydizer pipeline tuning.
    pub pipeline: Option<PipelineSettings>,
    /// One or more MinIO instance configurations.
    pub minio: Vec<MinioConfig>,
}

/// gRPC server listen address.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// Socket address to bind (e.g. `"0.0.0.0:50051"`).
    pub listen_addr: String,
}

/// Optional overrides for the xml-oxydizer validation pipeline.
#[derive(Debug, Deserialize)]
pub struct PipelineSettings {
    /// Number of rayon worker threads (`None` = rayon default).
    pub thread_count: Option<usize>,
    /// `BufReader` capacity in bytes per file stream.
    pub buf_reader_capacity: Option<usize>,
    /// Per-file diagnostics flush buffer size.
    pub diagnostics_buffer_size: Option<usize>,
}

/// Connection details for a single MinIO instance.
#[derive(Debug, Clone, Deserialize)]
pub struct MinioConfig {
    /// Logical name used to reference this instance in gRPC requests.
    pub name: String,
    /// MinIO endpoint URL (e.g. `"http://minio:9000"`).
    pub endpoint: String,
    /// S3 access key.
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// Optional region (unused by MinIO but kept for S3 compat).
    #[allow(dead_code)]
    pub region: Option<String>,
    /// Whether the endpoint uses TLS.
    pub secure: Option<bool>,
}

impl AppConfig {
    /// Reads and parses a TOML configuration file at `path`.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Converts the optional pipeline settings into an xml-oxydizer `PipelineConfig`.
    pub fn pipeline_config(&self) -> xml_oxydizer::pipeline::PipelineConfig {
        let defaults = xml_oxydizer::pipeline::PipelineConfig::default();
        match &self.pipeline {
            Some(p) => xml_oxydizer::pipeline::PipelineConfig {
                thread_count: p.thread_count,
                buf_reader_capacity: p.buf_reader_capacity.unwrap_or(defaults.buf_reader_capacity),
                diagnostics_buffer_size: p
                    .diagnostics_buffer_size
                    .unwrap_or(defaults.diagnostics_buffer_size),
            },
            None => defaults,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let toml = r#"
[server]
listen_addr = "0.0.0.0:50051"

[pipeline]
thread_count = 2
buf_reader_capacity = 32768
diagnostics_buffer_size = 128

[[minio]]
name = "main"
endpoint = "http://localhost:9000"
access_key = "admin"
secret_key = "secret"
region = "us-east-1"
secure = false

[[minio]]
name = "archive"
endpoint = "https://archive:9000"
access_key = "ak"
secret_key = "sk"
secure = true
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();

        assert_eq!(config.server.listen_addr, "0.0.0.0:50051");
        assert_eq!(config.minio.len(), 2);
        assert_eq!(config.minio[0].name, "main");
        assert_eq!(config.minio[1].name, "archive");
        assert_eq!(config.minio[1].secure, Some(true));

        let p = config.pipeline.as_ref().unwrap();
        assert_eq!(p.thread_count, Some(2));
        assert_eq!(p.buf_reader_capacity, Some(32768));
    }

    #[test]
    fn parse_minimal_config() {
        let toml = r#"
[server]
listen_addr = "127.0.0.1:8080"

[[minio]]
name = "default"
endpoint = "http://minio:9000"
access_key = "a"
secret_key = "s"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();

        assert!(config.pipeline.is_none());
        assert_eq!(config.minio.len(), 1);
        assert_eq!(config.minio[0].region, None);
        assert_eq!(config.minio[0].secure, None);
    }

    #[test]
    fn pipeline_config_defaults() {
        let toml = r#"
[server]
listen_addr = "0.0.0.0:50051"

[[minio]]
name = "x"
endpoint = "http://x:9000"
access_key = "a"
secret_key = "s"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let pc = config.pipeline_config();

        assert_eq!(pc.thread_count, None);
        assert_eq!(pc.buf_reader_capacity, 64 * 1024);
        assert_eq!(pc.diagnostics_buffer_size, 256);
    }

    #[test]
    fn pipeline_config_overrides() {
        let toml = r#"
[server]
listen_addr = "0.0.0.0:50051"

[pipeline]
thread_count = 8
buf_reader_capacity = 1024

[[minio]]
name = "x"
endpoint = "http://x:9000"
access_key = "a"
secret_key = "s"
"#;
        let config: AppConfig = toml::from_str(toml).unwrap();
        let pc = config.pipeline_config();

        assert_eq!(pc.thread_count, Some(8));
        assert_eq!(pc.buf_reader_capacity, 1024);
        assert_eq!(pc.diagnostics_buffer_size, 256);
    }
}
