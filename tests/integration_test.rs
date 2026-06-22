use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_channel::bounded;
use tokio_stream::StreamExt;
use tonic::Request;
use xml_oxydizer::pipeline::PipelineConfig;

use minio_xml_validator::grpc::service::ValidatorServiceImpl;
use minio_xml_validator::minio::client::MinioClientPool;
use minio_xml_validator::minio::download::ChannelReader;
use minio_xml_validator::proto;
use minio_xml_validator::proto::xml_validator_service_server::{
    XmlValidatorService, XmlValidatorServiceServer,
};
use minio_xml_validator::registry::ValidatorRegistry;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_minio_pool() -> Arc<MinioClientPool> {
    let configs = vec![minio_xml_validator::config::MinioConfig {
        name: "test".to_owned(),
        endpoint: "http://127.0.0.1:19000".to_owned(),
        access_key: "minioadmin".to_owned(),
        secret_key: "minioadmin".to_owned(),
        region: None,
        secure: Some(false),
    }];
    Arc::new(MinioClientPool::from_configs(&configs).unwrap())
}

fn test_registry() -> Arc<ValidatorRegistry> {
    let mut reg = ValidatorRegistry::new();
    minio_xml_validator::validators::register_all(&mut reg);
    Arc::new(reg)
}

fn test_service() -> ValidatorServiceImpl {
    ValidatorServiceImpl::new(
        test_minio_pool(),
        test_registry(),
        &PipelineConfig::default(),
    )
}

/// Starts a gRPC server on a random port and returns the connect address.
async fn start_grpc_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = test_service();
    let svc = XmlValidatorServiceServer::new(service);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

async fn connect_client(
    addr: SocketAddr,
) -> proto::xml_validator_service_client::XmlValidatorServiceClient<tonic::transport::Channel> {
    proto::xml_validator_service_client::XmlValidatorServiceClient::connect(format!(
        "http://{}",
        addr
    ))
    .await
    .unwrap()
}

fn file_request(
    minio_config: &str,
    bucket: &str,
    key: &str,
    validator: &str,
) -> proto::FileRequest {
    proto::FileRequest {
        minio_config_name: minio_config.to_owned(),
        bucket: bucket.to_owned(),
        object_key: key.to_owned(),
        validator_id: validator.to_owned(),
    }
}

/// Drains a ReceiverStream returned by the direct trait call.
async fn drain_receiver_stream(
    mut stream: tokio_stream::wrappers::ReceiverStream<Result<proto::ValidateResponse, tonic::Status>>,
) -> Vec<proto::ValidateResponse> {
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.unwrap());
    }
    out
}

/// Drains a tonic Streaming returned by the gRPC client.
async fn drain_grpc_stream(
    mut stream: tonic::Streaming<proto::ValidateResponse>,
) -> Vec<proto::ValidateResponse> {
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.unwrap());
    }
    out
}

// ---------------------------------------------------------------------------
// Direct trait-call tests (no TCP, fastest)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reject_empty_request() {
    let service = test_service();
    let req = Request::new(proto::ValidateRequest { files: vec![] });
    let err = service.validate_batch(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("no files"));
}

#[tokio::test]
async fn reject_unknown_minio_config() {
    let service = test_service();
    let req = Request::new(proto::ValidateRequest {
        files: vec![file_request(
            "nonexistent",
            "bucket",
            "key.xml",
            "example-catalog",
        )],
    });
    let err = service.validate_batch(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("nonexistent"));
}

#[tokio::test]
async fn reject_unknown_validator() {
    let service = test_service();
    let req = Request::new(proto::ValidateRequest {
        files: vec![file_request(
            "test",
            "bucket",
            "key.xml",
            "no-such-validator",
        )],
    });
    let err = service.validate_batch(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("no-such-validator"));
}

#[tokio::test]
async fn download_failure_returns_file_complete_error() {
    let service = test_service();
    let req = Request::new(proto::ValidateRequest {
        files: vec![file_request(
            "test",
            "bucket",
            "missing.xml",
            "example-catalog",
        )],
    });

    let response = service.validate_batch(req).await.unwrap();
    let responses = drain_receiver_stream(response.into_inner()).await;

    let file_completes: Vec<_> = responses
        .iter()
        .filter_map(|r| match &r.payload {
            Some(proto::validate_response::Payload::FileComplete(fc)) => Some(fc),
            _ => None,
        })
        .collect();

    assert!(
        !file_completes.is_empty(),
        "expected at least one FileComplete error, got: {:?}",
        responses
    );
    assert!(!file_completes[0].success);
    assert!(!file_completes[0].error_message.is_empty());
}

// ---------------------------------------------------------------------------
// Full gRPC round-trip tests (server + client over TCP)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_reject_empty_request() {
    let addr = start_grpc_server().await;
    let mut client = connect_client(addr).await;

    let err = client
        .validate_batch(proto::ValidateRequest { files: vec![] })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_reject_unknown_minio_config() {
    let addr = start_grpc_server().await;
    let mut client = connect_client(addr).await;

    let err = client
        .validate_batch(proto::ValidateRequest {
            files: vec![file_request("bad", "b", "k", "example-catalog")],
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("bad"));
}

#[tokio::test]
async fn grpc_reject_unknown_validator() {
    let addr = start_grpc_server().await;
    let mut client = connect_client(addr).await;

    let err = client
        .validate_batch(proto::ValidateRequest {
            files: vec![file_request("test", "b", "k", "nope")],
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("nope"));
}

#[tokio::test]
async fn grpc_download_failure_streams_file_complete() {
    let addr = start_grpc_server().await;
    let mut client = connect_client(addr).await;

    let response = client
        .validate_batch(proto::ValidateRequest {
            files: vec![file_request(
                "test",
                "bucket",
                "nonexistent.xml",
                "example-catalog",
            )],
        })
        .await
        .unwrap();

    let responses = drain_grpc_stream(response.into_inner()).await;

    let has_error = responses.iter().any(|r| {
        matches!(
            &r.payload,
            Some(proto::validate_response::Payload::FileComplete(fc)) if !fc.success
        )
    });
    assert!(has_error, "expected FileComplete error, got: {:?}", responses);
}

// ---------------------------------------------------------------------------
// Pipeline integration tests (ChannelReader → xml-oxydizer → diagnostics)
// ---------------------------------------------------------------------------

#[test]
fn pipeline_valid_xml_no_diagnostics() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};

    let tree = test_registry().get("example-catalog").unwrap();

    let (tx, rx) = bounded(4);
    tx.send(Ok(Bytes::from(r#"<catalog version="1">"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"<entry id="a"/>"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"<entry id="b"/>"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"</catalog>"#))).unwrap();
    drop(tx);

    let (diag_tx, diag_rx) = bounded::<Diagnostic>(64);
    let errors = run_pipeline(
        vec![FileInfo {
            filename: "streamed.xml".to_owned(),
            descriptors: tree,
            stream_factory: Box::new(move || {
                Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
            }),
        }],
        diag_tx,
        &PipelineConfig::default(),
    );
    assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(diagnostics.is_empty(), "unexpected diagnostics: {:?}", diagnostics);
}

#[test]
fn pipeline_invalid_xml_produces_diagnostics() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};

    let tree = test_registry().get("example-catalog").unwrap();

    let (tx, rx) = bounded(4);
    tx.send(Ok(Bytes::from(r#"<catalog>"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"<entry/>"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"</catalog>"#))).unwrap();
    drop(tx);

    let (diag_tx, diag_rx) = bounded::<Diagnostic>(64);
    let errors = run_pipeline(
        vec![FileInfo {
            filename: "bad.xml".to_owned(),
            descriptors: tree,
            stream_factory: Box::new(move || {
                Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
            }),
        }],
        diag_tx,
        &PipelineConfig::default(),
    );
    assert!(errors.is_empty(), "pipeline errors: {:?}", errors);

    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(
        diagnostics.len() >= 2,
        "expected at least 2 diagnostics (missing version + missing id), got: {:?}",
        diagnostics
    );

    let rule_names: Vec<_> = diagnostics.iter().map(|d| d.rule_name.as_str()).collect();
    assert!(rule_names.contains(&"require_attr"));
}

#[test]
fn pipeline_multiple_files_parallel() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};
    use xml_oxydizer::rule::Rule;

    let tree = test_registry().get("example-catalog").unwrap();

    let files: Vec<FileInfo<Box<dyn Rule>>> = (0..10)
        .map(|i| {
            let (tx, rx) = bounded(4);
            let xml = format!(
                r#"<catalog version="{}"><entry id="e{}"/></catalog>"#,
                i, i
            );
            tx.send(Ok(Bytes::from(xml))).unwrap();
            drop(tx);

            FileInfo {
                filename: format!("file_{}.xml", i),
                descriptors: Arc::clone(&tree),
                stream_factory: Box::new(move || {
                    Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
                }),
            }
        })
        .collect();

    let (diag_tx, diag_rx) = bounded::<Diagnostic>(256);
    let errors = run_pipeline(files, diag_tx, &PipelineConfig::default());
    assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(diagnostics.is_empty(), "unexpected diagnostics: {:?}", diagnostics);
}

#[test]
fn pipeline_chunked_across_tag_boundaries() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};

    let tree = test_registry().get("example-catalog").unwrap();

    let (tx, rx) = bounded(8);
    tx.send(Ok(Bytes::from(r#"<cata"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"log ver"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"sion="1"><en"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"try id="x"#))).unwrap();
    tx.send(Ok(Bytes::from(r#""/></ca"#))).unwrap();
    tx.send(Ok(Bytes::from(r#"talog>"#))).unwrap();
    drop(tx);

    let (diag_tx, diag_rx) = bounded::<Diagnostic>(64);
    let errors = run_pipeline(
        vec![FileInfo {
            filename: "chunked.xml".to_owned(),
            descriptors: tree,
            stream_factory: Box::new(move || {
                Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
            }),
        }],
        diag_tx,
        &PipelineConfig::default(),
    );
    assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(diagnostics.is_empty(), "unexpected diagnostics: {:?}", diagnostics);
}

#[test]
fn pipeline_download_error_midstream() {
    use std::io::Read;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};

    let tree = test_registry().get("example-catalog").unwrap();

    let (tx, rx) = bounded(4);
    tx.send(Ok(Bytes::from(r#"<catalog version="1">"#))).unwrap();
    tx.send(Err("connection reset".to_owned())).unwrap();
    drop(tx);

    let (diag_tx, _diag_rx) = bounded::<xml_oxydizer::diagnostic::Diagnostic>(64);
    let errors = run_pipeline(
        vec![FileInfo {
            filename: "interrupted.xml".to_owned(),
            descriptors: tree,
            stream_factory: Box::new(move || {
                Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
            }),
        }],
        diag_tx,
        &PipelineConfig::default(),
    );
    assert_eq!(errors.len(), 1, "expected one pipeline error for interrupted download");
}

#[test]
fn pipeline_empty_catalog_warns() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline};

    let tree = test_registry().get("example-catalog").unwrap();

    let (tx, rx) = bounded(4);
    tx.send(Ok(Bytes::from(r#"<catalog version="1"/>"#))).unwrap();
    drop(tx);

    let (diag_tx, diag_rx) = bounded::<Diagnostic>(64);
    let errors = run_pipeline(
        vec![FileInfo {
            filename: "empty.xml".to_owned(),
            descriptors: tree,
            stream_factory: Box::new(move || {
                Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
            }),
        }],
        diag_tx,
        &PipelineConfig::default(),
    );
    assert!(errors.is_empty());

    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(
        diagnostics.iter().any(|d| d.rule_name == "require_children"),
        "expected require_children warning, got: {:?}",
        diagnostics
    );
}

#[test]
fn pipeline_streaming_channel_fed() {
    use std::io::Read;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, run_pipeline_streaming};
    use xml_oxydizer::rule::Rule;

    let tree = test_registry().get("example-catalog").unwrap();

    let (file_tx, file_rx) = bounded::<FileInfo<Box<dyn Rule>>>(4);
    let (diag_tx, diag_rx) = bounded::<Diagnostic>(256);

    let tree_clone = Arc::clone(&tree);
    let sender = std::thread::spawn(move || {
        for i in 0..5 {
            let (tx, rx) = bounded(4);
            let xml = format!(
                r#"<catalog version="{}"><entry id="e{}"/></catalog>"#,
                i, i
            );
            tx.send(Ok(Bytes::from(xml))).unwrap();
            drop(tx);

            file_tx
                .send(FileInfo {
                    filename: format!("stream_{}.xml", i),
                    descriptors: Arc::clone(&tree_clone),
                    stream_factory: Box::new(move || {
                        Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
                    }),
                })
                .unwrap();
        }
    });

    let errors = run_pipeline_streaming(file_rx, diag_tx, &PipelineConfig::default());
    sender.join().unwrap();

    assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
    let diagnostics: Vec<_> = diag_rx.try_iter().collect();
    assert!(diagnostics.is_empty(), "unexpected diagnostics: {:?}", diagnostics);
}
