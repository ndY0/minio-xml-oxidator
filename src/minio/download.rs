//! Streaming object download and sync `Read` adapter for xml-oxydizer.
//!
//! Instead of downloading the entire object into memory, we pipe async
//! chunks from MinIO through a bounded crossbeam channel into a
//! [`ChannelReader`] that implements `std::io::Read`. This preserves
//! the O(depth) memory footprint of the streaming parser — only a
//! bounded number of chunks are buffered at any time.

use std::io::{self, Cursor, Read};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use futures_util::StreamExt;
use minio::s3::types::S3Api;
use minio::s3::MinioClient;
use tokio::runtime::Handle;

/// Number of chunks buffered between the async download task and the
/// sync reader. Tuned for throughput without excessive memory use.
const CHANNEL_BUFFER_CHUNKS: usize = 16;

/// A synchronous `Read` adapter backed by a crossbeam channel.
///
/// A tokio task pushes `Bytes` chunks into the channel. The rayon
/// worker thread consumes them through the `Read` impl. Backpressure
/// is provided by the bounded channel — the producer blocks when the
/// buffer is full.
pub struct ChannelReader {
    rx: Receiver<Result<Bytes, String>>,
    current: Cursor<Bytes>,
}

impl ChannelReader {
    /// Creates a new reader that pulls chunks from the given channel.
    pub fn new(rx: Receiver<Result<Bytes, String>>) -> Self {
        Self {
            rx,
            current: Cursor::new(Bytes::new()),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.current.read(buf)?;
            if n > 0 {
                return Ok(n);
            }

            match self.rx.recv() {
                Ok(Ok(bytes)) => {
                    self.current = Cursor::new(bytes);
                }
                Ok(Err(e)) => {
                    return Err(io::Error::other(e));
                }
                Err(_) => return Ok(0),
            }
        }
    }
}

/// Starts an async streaming download and returns a `stream_factory`
/// closure for xml-oxydizer's `FileInfo`.
///
/// The download begins immediately: a tokio task is spawned that
/// streams chunks from the MinIO response into a bounded crossbeam
/// channel. The returned closure, when called on a rayon worker thread,
/// returns a [`ChannelReader`] connected to that channel.
///
/// # Memory usage
///
/// At most `CHANNEL_BUFFER_CHUNKS` chunks are held in memory at any
/// time (plus whatever `quick-xml` buffers internally). This is
/// bounded and independent of the total object size.
pub fn start_streaming_download(
    client: &MinioClient,
    bucket: &str,
    object_key: &str,
    handle: &Handle,
) -> Result<Box<dyn FnOnce() -> Box<dyn Read + Send> + Send>, anyhow::Error> {
    let (tx, rx): (Sender<Result<Bytes, String>>, _) = bounded(CHANNEL_BUFFER_CHUNKS);

    let client = client.clone();
    let bucket = bucket.to_owned();
    let object_key = object_key.to_owned();

    handle.spawn(async move {
        let result = stream_object(&client, &bucket, &object_key, &tx).await;
        if let Err(e) = result {
            let _ = tx.send(Err(e.to_string()));
        }
    });

    Ok(Box::new(move || {
        Box::new(ChannelReader::new(rx)) as Box<dyn Read + Send>
    }))
}

/// Streams object content chunk by chunk into the channel.
async fn stream_object(
    client: &MinioClient,
    bucket: &str,
    object_key: &str,
    tx: &Sender<Result<Bytes, String>>,
) -> anyhow::Result<()> {
    let response = client
        .get_object(bucket, object_key)
        .map_err(|e| anyhow::anyhow!("invalid get_object params: {e}"))?
        .build()
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("get_object failed for {bucket}/{object_key}: {e}"))?;

    let content = response
        .content()
        .map_err(|e| anyhow::anyhow!("failed to get content for {bucket}/{object_key}: {e}"))?;

    let (mut stream, _size) = content
        .to_stream()
        .await
        .map_err(|e| anyhow::anyhow!("failed to open stream for {bucket}/{object_key}: {e}"))?;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(bytes) => {
                if bytes.is_empty() {
                    break;
                }
                if tx.send(Ok(bytes)).is_err() {
                    break;
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e.to_string()));
                break;
            }
        }
    }

    Ok(())
}

/// Wraps pre-downloaded bytes in a closure suitable for `FileInfo::stream_factory`.
///
/// Useful for tests or small payloads where full download is acceptable.
#[cfg(test)]
pub fn make_stream_factory(data: Vec<u8>) -> Box<dyn FnOnce() -> Box<dyn Read + Send> + Send> {
    Box::new(move || Box::new(Cursor::new(data)) as Box<dyn Read + Send>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn stream_factory_returns_correct_data() {
        let data = b"<root>hello</root>".to_vec();
        let factory = make_stream_factory(data.clone());
        let mut reader = factory();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn stream_factory_empty_data() {
        let factory = make_stream_factory(Vec::new());
        let mut reader = factory();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn channel_reader_single_chunk() {
        let (tx, rx) = bounded(4);
        tx.send(Ok(Bytes::from("hello world"))).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
    }

    #[test]
    fn channel_reader_multiple_chunks() {
        let (tx, rx) = bounded(4);
        tx.send(Ok(Bytes::from("<root>"))).unwrap();
        tx.send(Ok(Bytes::from("<child/>"))).unwrap();
        tx.send(Ok(Bytes::from("</root>"))).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"<root><child/></root>");
    }

    #[test]
    fn channel_reader_empty_channel() {
        let (tx, rx) = bounded(4);
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn channel_reader_propagates_error() {
        let (tx, rx) = bounded(4);
        tx.send(Ok(Bytes::from("partial"))).unwrap();
        tx.send(Err("download failed".to_owned())).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = vec![0u8; 1024];

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"partial");

        let err = reader.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("download failed"));
    }

    #[test]
    fn channel_reader_small_buffer_reads() {
        let (tx, rx) = bounded(4);
        tx.send(Ok(Bytes::from("abcdef"))).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = [0u8; 3];

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"abc");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"def");

        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn channel_reader_with_xml_pipeline() {
        use std::sync::Arc;
        use crossbeam_channel::bounded as cb_bounded;
        use xml_oxydizer::diagnostic::Diagnostic;
        use xml_oxydizer::pipeline::{FileInfo, PipelineConfig, run_pipeline};
        use xml_oxydizer::rule::Rule;
        use xml_oxydizer::tree::builder::TreeBuilder;

        let tree = Arc::new(
            TreeBuilder::<Box<dyn Rule>>::new("root")
                .streaming()
                .build()
                .unwrap(),
        );

        let (chunk_tx, chunk_rx) = bounded(4);
        chunk_tx.send(Ok(Bytes::from("<root>"))).unwrap();
        chunk_tx.send(Ok(Bytes::from("<child/>"))).unwrap();
        chunk_tx.send(Ok(Bytes::from("</root>"))).unwrap();
        drop(chunk_tx);

        let (diag_tx, diag_rx) = cb_bounded::<Diagnostic>(64);
        let errors = run_pipeline(
            vec![FileInfo {
                filename: "streamed.xml".to_owned(),
                descriptors: tree,
                stream_factory: Box::new(move || {
                    Box::new(ChannelReader::new(chunk_rx)) as Box<dyn Read + Send>
                }),
            }],
            diag_tx,
            &PipelineConfig::default(),
        );
        assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
        let diags: Vec<_> = diag_rx.try_iter().collect();
        assert!(diags.is_empty());
    }
}
