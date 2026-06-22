# minio-xml-validator

A gRPC server that downloads XML files from MinIO and validates them using the [xml-oxydizer](../xml-validator) streaming validation library. Designed to be called from a Java application that submits file references and receives diagnostics asynchronously.

## Features

- **gRPC API** with server-side streaming — submit a batch of files, receive diagnostics as they are produced.
- **Multiple MinIO instances** — configure several MinIO endpoints in a single TOML file and reference them by name.
- **Validator registry** — each file carries a validator ID that maps to a pre-built xml-oxydizer `DescriptorTree`.
- **True streaming downloads** — bytes flow from MinIO through a bounded crossbeam channel directly into the xml-oxydizer parser. No full file download to memory; memory usage is O(depth) + a fixed chunk buffer.
- **Pipelined processing** — file downloads overlap with validation via `run_pipeline_streaming`.
- **Async/sync bridge** — tokio tasks for async MinIO I/O, rayon thread pool for parallel XML parsing, crossbeam channels bridged to gRPC streams.

## Architecture

```
Java gRPC Client
  |
  | ValidateBatch(ValidateRequest)
  v
ValidatorServiceImpl::validate_batch()
  |
  |-- TASK 1 (tokio::spawn)       Start streaming downloads from MinIO
  |     Spawns async task per file → ChannelReader pipe
  |     Creates FileInfo with streaming stream_factory
  |     Sends via crossbeam channel → pipeline
  |
  |-- TASK 2 (spawn_blocking)     run_pipeline_streaming()
  |     rayon processes files in parallel
  |     Sends Diagnostics via crossbeam channel
  |
  |-- TASK 3 (spawn_blocking)     Diagnostic bridge
  |     Drains crossbeam Receiver
  |     Forwards to gRPC stream via tokio mpsc
  |
  v
Stream<ValidateResponse> → Java client
```

## Configuration

Create a `config.toml`:

```toml
[server]
listen_addr = "0.0.0.0:50051"

[pipeline]
thread_count = 4              # rayon threads (default: num CPUs)
buf_reader_capacity = 65536   # per-file buffer (default: 64KB)
diagnostics_buffer_size = 256 # flush threshold (default: 256)

[[minio]]
name = "main"
endpoint = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
region = "us-east-1"
secure = false

[[minio]]
name = "archive"
endpoint = "https://archive-minio:9000"
access_key = "archiveadmin"
secret_key = "archiveadmin"
secure = true
```

## gRPC API

Defined in [`proto/validator.proto`](proto/validator.proto):

```protobuf
service XmlValidatorService {
  rpc ValidateBatch(ValidateRequest) returns (stream ValidateResponse);
}
```

### Request

```protobuf
message ValidateRequest {
  repeated FileRequest files = 1;
}

message FileRequest {
  string minio_config_name = 1;  // matches [[minio]] name
  string bucket = 2;
  string object_key = 3;
  string validator_id = 4;       // maps to a registered DescriptorTree
}
```

### Response (streamed)

Each `ValidateResponse` is either a diagnostic or a file-completion marker:

```protobuf
message ValidateResponse {
  oneof payload {
    DiagnosticMessage diagnostic = 1;   // rule finding
    FileComplete file_complete = 2;     // file done or errored
  }
}
```

## Adding Validators

Validators are registered at startup in `src/validators/mod.rs`. Each one builds a `DescriptorTree` describing the expected XML structure and validation rules:

```rust
fn build_my_validator() -> DescriptorTree<Box<dyn Rule>> {
    TreeBuilder::new("root")
        .streaming()
        .rule(Box::new(MyRule { ... }) as Box<dyn Rule>)
        .node("child")
            .streaming()
            .rule(Box::new(OtherRule { ... }) as Box<dyn Rule>)
            .done()
        .build()
        .expect("invalid tree")
}
```

Then register it in `register_all`:

```rust
pub fn register_all(registry: &mut ValidatorRegistry) {
    registry.register("my-validator", build_my_validator());
}
```

The Java client references this validator by passing `validator_id = "my-validator"` in the `FileRequest`.

## Build & Test

```bash
# Build
cargo build

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings

# Run the server
cargo run -- config.toml
```

## Testing with grpcurl

```bash
# List services
grpcurl -plaintext localhost:50051 list

# Call ValidateBatch
grpcurl -plaintext -d '{
  "files": [{
    "minio_config_name": "main",
    "bucket": "xml-files",
    "object_key": "catalog.xml",
    "validator_id": "example-catalog"
  }]
}' localhost:50051 validator.XmlValidatorService/ValidateBatch
```

## Project Structure

```
src/
  main.rs               Tokio entrypoint, wires config → pool → registry → server
  config.rs             TOML configuration parsing
  grpc/
    mod.rs
    service.rs          ValidatorServiceImpl — 3-task orchestration
    convert.rs          Diagnostic → protobuf message conversion
  minio/
    mod.rs
    client.rs           MinioClientPool — HashMap<name, MinioClient>
    download.rs         Streaming ChannelReader + async download pipe
  registry/
    mod.rs              ValidatorRegistry — HashMap<id, Arc<DescriptorTree>>
  validators/
    mod.rs              Concrete validator definitions, register_all()
proto/
  validator.proto       gRPC service definition
config.toml             Example configuration
```

## Dependencies

| Crate | Purpose |
|---|---|
| `xml-oxydizer` | Streaming XML validation (local path dependency) |
| `tonic` / `prost` | gRPC server and protobuf codegen |
| `tokio` | Async runtime |
| `minio` | S3-compatible object storage client (rustls) |
| `crossbeam-channel` | Sync channels bridging rayon and tokio |
| `serde` / `toml` | Configuration parsing |
| `tracing` | Structured logging |
| `anyhow` / `thiserror` | Error handling |

## Minimum Supported Rust Version

Rust **edition 2024** (requires rustc 1.85+).
