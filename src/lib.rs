//! gRPC server library for downloading XML files from MinIO and validating
//! them using the `xml-oxydizer` streaming validation library.

pub mod config;
pub mod grpc;
pub mod minio;
pub mod registry;
pub mod validators;

/// Generated protobuf/gRPC types from `proto/validator.proto`.
pub mod proto {
    tonic::include_proto!("validator");
}
