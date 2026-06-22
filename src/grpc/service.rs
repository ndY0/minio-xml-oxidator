//! Core gRPC service that orchestrates MinIO download, xml-oxydizer
//! validation, and diagnostic streaming back to the client.
//!
//! Each `validate_batch` call spawns three concurrent tasks:
//! 1. **Download** (async) — fetches files from MinIO, feeds `FileInfo` into the pipeline.
//! 2. **Pipeline** (blocking) — runs `run_pipeline_streaming` on a dedicated thread.
//! 3. **Diagnostic bridge** (blocking) — drains the crossbeam diagnostic channel
//!    and forwards messages to the gRPC response stream via `blocking_send`.

use std::sync::Arc;

use crossbeam_channel::bounded;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use xml_oxydizer::pipeline::{FileInfo, PipelineConfig, run_pipeline_streaming};
use xml_oxydizer::rule::Rule;

use crate::grpc::convert::diagnostic_to_proto;
use crate::minio::client::MinioClientPool;
use crate::minio::download;
use crate::proto;
use crate::proto::xml_validator_service_server::XmlValidatorService;
use crate::registry::ValidatorRegistry;

/// Implements the `XmlValidatorService` gRPC service.
///
/// Holds shared references to the MinIO client pool, validator registry,
/// and pipeline configuration fields (stored individually since
/// `PipelineConfig` is not `Clone`).
pub struct ValidatorServiceImpl {
    minio_pool: Arc<MinioClientPool>,
    registry: Arc<ValidatorRegistry>,
    thread_count: Option<usize>,
    buf_reader_capacity: usize,
    diagnostics_buffer_size: usize,
}

impl ValidatorServiceImpl {
    /// Creates a new service instance from shared dependencies.
    pub fn new(
        minio_pool: Arc<MinioClientPool>,
        registry: Arc<ValidatorRegistry>,
        pipeline_config: &PipelineConfig,
    ) -> Self {
        Self {
            minio_pool,
            registry,
            thread_count: pipeline_config.thread_count,
            buf_reader_capacity: pipeline_config.buf_reader_capacity,
            diagnostics_buffer_size: pipeline_config.diagnostics_buffer_size,
        }
    }

    /// Reconstructs a fresh `PipelineConfig` for each request.
    fn build_pipeline_config(&self) -> PipelineConfig {
        PipelineConfig {
            thread_count: self.thread_count,
            buf_reader_capacity: self.buf_reader_capacity,
            diagnostics_buffer_size: self.diagnostics_buffer_size,
        }
    }
}

#[tonic::async_trait]
impl XmlValidatorService for ValidatorServiceImpl {
    type ValidateBatchStream = ReceiverStream<Result<proto::ValidateResponse, Status>>;

    /// Validates a batch of XML files from MinIO, streaming diagnostics back
    /// as they are produced by the xml-oxydizer pipeline.
    async fn validate_batch(
        &self,
        request: Request<proto::ValidateRequest>,
    ) -> Result<Response<Self::ValidateBatchStream>, Status> {
        let req = request.into_inner();

        if req.files.is_empty() {
            return Err(Status::invalid_argument("no files provided"));
        }

        // Validate all requests upfront before starting any work.
        for file_req in &req.files {
            if self.minio_pool.get(&file_req.minio_config_name).is_none() {
                return Err(Status::not_found(format!(
                    "unknown minio config '{}'",
                    file_req.minio_config_name
                )));
            }
            if self.registry.get(&file_req.validator_id).is_none() {
                return Err(Status::not_found(format!(
                    "unknown validator '{}'",
                    file_req.validator_id
                )));
            }
        }

        let (grpc_tx, grpc_rx) = mpsc::channel::<Result<proto::ValidateResponse, Status>>(256);

        let file_count = req.files.len();
        let (file_tx, file_rx) = bounded::<FileInfo<Box<dyn Rule>>>(file_count);
        let (diag_tx, diag_rx) = bounded::<xml_oxydizer::diagnostic::Diagnostic>(1024);

        let minio_pool = Arc::clone(&self.minio_pool);
        let registry = Arc::clone(&self.registry);
        let grpc_tx_download = grpc_tx.clone();
        let handle = tokio::runtime::Handle::current();

        // TASK 1: Start streaming downloads from MinIO and feed into pipeline.
        // Each file gets a ChannelReader that streams bytes directly from
        // MinIO into the xml-oxydizer parser — no full download to Vec<u8>.
        tokio::spawn(async move {
            for file_req in req.files {
                let client = match minio_pool.get(&file_req.minio_config_name) {
                    Some(c) => c,
                    None => continue,
                };
                let tree = match registry.get(&file_req.validator_id) {
                    Some(t) => t,
                    None => continue,
                };

                let filename = format!("{}/{}", file_req.bucket, file_req.object_key);

                match download::start_streaming_download(
                    client,
                    &file_req.bucket,
                    &file_req.object_key,
                    &handle,
                ) {
                    Ok(stream_factory) => {
                        let file_info = FileInfo {
                            filename,
                            descriptors: tree,
                            stream_factory,
                        };
                        if file_tx.send(file_info).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let resp = proto::ValidateResponse {
                            payload: Some(proto::validate_response::Payload::FileComplete(
                                proto::FileComplete {
                                    filename,
                                    success: false,
                                    error_message: e.to_string(),
                                },
                            )),
                        };
                        if grpc_tx_download.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            drop(file_tx);
        });

        // TASK 2: Run xml-oxydizer pipeline (blocking, on dedicated thread)
        let pipeline_config = self.build_pipeline_config();
        let grpc_tx_pipeline = grpc_tx.clone();
        tokio::task::spawn_blocking(move || {
            let errors = run_pipeline_streaming(file_rx, diag_tx, &pipeline_config);

            for err in errors {
                let resp = proto::ValidateResponse {
                    payload: Some(proto::validate_response::Payload::FileComplete(
                        proto::FileComplete {
                            filename: err.to_string(),
                            success: false,
                            error_message: err.to_string(),
                        },
                    )),
                };
                let _ = grpc_tx_pipeline.blocking_send(Ok(resp));
            }
        });

        // TASK 3: Bridge crossbeam diagnostics to gRPC response stream
        let grpc_tx_diag = grpc_tx;
        tokio::task::spawn_blocking(move || {
            while let Ok(diagnostic) = diag_rx.recv() {
                let filename = xml_oxydizer::tree::path::format_path(&diagnostic.element_path);
                let msg = diagnostic_to_proto(&diagnostic, &filename);
                let resp = proto::ValidateResponse {
                    payload: Some(proto::validate_response::Payload::Diagnostic(msg)),
                };
                if grpc_tx_diag.blocking_send(Ok(resp)).is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}
