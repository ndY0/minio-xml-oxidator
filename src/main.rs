//! Binary entrypoint for the minio-xml-validator gRPC server.

use std::sync::Arc;

use tracing::info;

use minio_xml_validator::config;
use minio_xml_validator::grpc;
use minio_xml_validator::minio;
use minio_xml_validator::proto;
use minio_xml_validator::registry;
use minio_xml_validator::validators;

/// Boots the gRPC server: loads config, initializes MinIO clients and
/// the validator registry, then serves on the configured address.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    info!(path = %config_path, "loading configuration");
    let config = config::AppConfig::load(&config_path)?;

    let minio_pool = Arc::new(minio::client::MinioClientPool::from_configs(&config.minio)?);
    info!(count = config.minio.len(), "minio clients initialized");

    let mut validator_registry = registry::ValidatorRegistry::new();
    validators::register_all(&mut validator_registry);
    let validator_registry = Arc::new(validator_registry);

    let pipeline_config = config.pipeline_config();
    let service =
        grpc::service::ValidatorServiceImpl::new(minio_pool, validator_registry, &pipeline_config);

    let addr = config.server.listen_addr.parse()?;
    info!(%addr, "starting gRPC server");

    tonic::transport::Server::builder()
        .add_service(proto::xml_validator_service_server::XmlValidatorServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
