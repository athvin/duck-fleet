//! Private, bounded job artifacts. Object failures never classify a SQL outcome.
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use axum::body::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig,
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    path::Path,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    future::Future,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, timeout_at};

pub(crate) const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_SCHEMA_BYTES: usize = 256 * 1024;
pub(crate) const MAX_CHUNKS: usize = 4096;
const MAX_BUFFERED_BATCHES: usize = 32;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtifactError {
    Unavailable,
    Corrupt,
    TooLarge,
    Canceled,
    TimedOut,
    Encoding,
}
impl ArtifactError {
    pub fn kind(self) -> &'static str {
        match self {
            Self::Unavailable => "result_store_unavailable",
            Self::Corrupt => "result_corrupt",
            Self::TooLarge => "result_too_large",
            Self::Canceled => "result_canceled",
            Self::TimedOut => "result_delivery_timeout",
            Self::Encoding => "result_encoding_failed",
        }
    }
}
impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.kind())
    }
}
impl std::error::Error for ArtifactError {}
type Result<T> = std::result::Result<T, ArtifactError>;

/// Authentication is selected explicitly. Never fall through to a node's IAM
/// credentials because an artifact-specific setting was absent or malformed.
enum Authentication {
    Static {
        key: String,
        secret: String,
        token: Option<String>,
    },
    PodIdentity {
        uri: String,
        token_file: String,
    },
}
pub(crate) struct StoreConfig {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Option<String>,
    allow_http: bool,
    authentication: Authentication,
}

fn required(
    read: &impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
) -> anyhow::Result<String> {
    let value = read(name)?.ok_or_else(|| anyhow::anyhow!("{name} is required"))?;
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 8192,
        "{name} is empty or too long"
    );
    Ok(value)
}
fn optional(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => anyhow::bail!("{name} must be valid Unicode"),
    }
}
fn location(value: &str) -> anyhow::Result<(String, String)> {
    let (bucket, prefix) = value
        .strip_prefix("s3://")
        .and_then(|s| s.split_once('/'))
        .ok_or_else(|| anyhow::anyhow!("result data path must be s3://bucket/nonempty-prefix/"))?;
    let prefix = prefix.trim_end_matches('/');
    anyhow::ensure!(
        !bucket.is_empty()
            && bucket.len() <= 63
            && bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            && !prefix.is_empty()
            && prefix.len() <= 512
            && prefix.split('/').all(|segment| !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))),
        "result data path contains an invalid bucket or prefix"
    );
    Ok((bucket.to_owned(), prefix.to_owned()))
}
fn overlaps(left: &str, right: &str) -> bool {
    left == right
        || left.strip_prefix(right).is_some_and(|s| s.starts_with('/'))
        || right.strip_prefix(left).is_some_and(|s| s.starts_with('/'))
}
impl StoreConfig {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        Self::from_lookup(optional)
    }
    // Inject the source for configuration tests without changing process-wide
    // credentials in a parallel test suite. The production source is env only.
    fn from_lookup(
        read: impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(path) = read("DUCKFLEET_RESULT_DATA_PATH")? else {
            return Ok(None);
        };
        let (bucket, prefix) = location(&path)?;
        if let Some(lake) = read("DUCKFLEET_RESULT_LAKE_DATA_PATH")?.or(read("S3_DATA_PATH")?) {
            let (lake_bucket, lake_prefix) = location(&lake)?;
            anyhow::ensure!(
                bucket != lake_bucket || !overlaps(&prefix, &lake_prefix),
                "result and lake prefixes must be separate; lake maintenance must not own result objects"
            );
        }
        let allow_http = match read("DUCKFLEET_RESULT_ALLOW_HTTP")?.as_deref() {
            Some("true") => true,
            Some("false") | None => false,
            _ => anyhow::bail!("DUCKFLEET_RESULT_ALLOW_HTTP must be true or false"),
        };
        let endpoint = read("DUCKFLEET_RESULT_ENDPOINT")?;
        if let Some(endpoint) = &endpoint {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| anyhow::anyhow!("invalid result endpoint"))?;
            anyhow::ensure!(
                url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/"
                    && (url.scheme() == "https" || (allow_http && url.scheme() == "http")),
                "result endpoint must be an HTTPS origin, or explicitly allowed HTTP origin"
            );
        }
        let authentication = match required(&read, "DUCKFLEET_RESULT_AUTH_MODE")?.as_str() {
            "static" => Authentication::Static {
                key: required(&read, "DUCKFLEET_RESULT_ACCESS_KEY")?,
                secret: required(&read, "DUCKFLEET_RESULT_SECRET_KEY")?,
                token: read("DUCKFLEET_RESULT_SESSION_TOKEN")?,
            },
            "pod_identity" => {
                let uri = required(&read, "AWS_CONTAINER_CREDENTIALS_FULL_URI")?;
                let url = reqwest::Url::parse(&uri)
                    .map_err(|_| anyhow::anyhow!("invalid Pod Identity endpoint"))?;
                anyhow::ensure!(
                    matches!(url.scheme(), "http" | "https")
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.fragment().is_none()
                        && matches!(url.host_str(), Some("169.254.170.23" | "[fd00:ec2::23]")),
                    "artifact Pod Identity requires the EKS credential endpoint"
                );
                Authentication::PodIdentity {
                    uri,
                    token_file: required(&read, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")?,
                }
            }
            _ => anyhow::bail!("DUCKFLEET_RESULT_AUTH_MODE must be static or pod_identity"),
        };
        let region = read("AWS_REGION")?.unwrap_or_else(|| "us-east-1".into());
        anyhow::ensure!(
            !region.is_empty()
                && region.len() <= 64
                && region
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid artifact region"
        );
        Ok(Some(Self {
            bucket,
            prefix,
            region,
            endpoint,
            allow_http,
            authentication,
        }))
    }
    pub fn build(self) -> anyhow::Result<Store> {
        let options = ClientOptions::new()
            .with_allow_http(self.allow_http)
            .with_connect_timeout(Duration::from_secs(3))
            .with_timeout(IO_TIMEOUT);
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(self.bucket)
            .with_region(self.region)
            .with_virtual_hosted_style_request(false)
            .with_client_options(options)
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: IO_TIMEOUT,
                ..Default::default()
            });
        if let Some(endpoint) = self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        builder = match self.authentication {
            Authentication::Static { key, secret, token } => {
                let mut builder = builder
                    .with_access_key_id(key)
                    .with_secret_access_key(secret);
                if let Some(token) = token {
                    builder = builder.with_token(token);
                }
                builder
            }
            Authentication::PodIdentity { uri, token_file } => builder
                .with_config(AmazonS3ConfigKey::ContainerCredentialsFullUri, uri)
                .with_config(
                    AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
                    token_file,
                ),
        };
        let object = builder
            .build()
            .map_err(|_| anyhow::anyhow!("artifact object-store configuration failed"))?;
        Ok(Store {
            object: Arc::new(object),
            prefix: self.prefix,
        })
    }
}

#[derive(Clone)]
pub(crate) struct Store {
    object: Arc<dyn ObjectStore>,
    prefix: String,
}
impl Store {
    fn key(&self, relative: &str) -> Result<Path> {
        // Journal-generated relative keys only. Defense in depth against corrupt
        // metadata: no URL, empty segment, traversal, escaped path, or backslash.
        if relative.len() > 512
            || relative.split('/').any(|s| {
                s.is_empty()
                    || s == "."
                    || s == ".."
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            })
        {
            return Err(ArtifactError::Corrupt);
        }
        Path::parse(format!("{}/{relative}", self.prefix)).map_err(|_| ArtifactError::Corrupt)
    }
    pub async fn put(
        &self,
        relative: &str,
        bytes: Bytes,
        expected_sha256: &str,
        deadline: Instant,
        canceled: &AtomicBool,
    ) -> Result<()> {
        if bytes.len() > MAX_CHUNK_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        if sha256(&bytes) != expected_sha256 {
            return Err(ArtifactError::Corrupt);
        }
        let key = self.key(relative)?;
        bounded(deadline, canceled, async {
            self.object
                .put(&key, bytes.into())
                .await
                .map_err(|_| ArtifactError::Unavailable)?;
            Ok(())
        })
        .await
    }
    pub async fn get(
        &self,
        relative: &str,
        expected_bytes: u64,
        expected_sha256: &str,
        maximum: usize,
        deadline: Instant,
        canceled: &AtomicBool,
    ) -> Result<Bytes> {
        let expected = usize::try_from(expected_bytes).map_err(|_| ArtifactError::TooLarge)?;
        if expected > maximum.min(MAX_CHUNK_BYTES) {
            return Err(ArtifactError::TooLarge);
        }
        let key = self.key(relative)?;
        bounded(deadline, canceled, async {
            let result = self
                .object
                .get(&key)
                .await
                .map_err(|_| ArtifactError::Unavailable)?;
            if result.meta.size != expected_bytes || result.range != (0..expected_bytes) {
                return Err(ArtifactError::Corrupt);
            }
            let mut output = Vec::with_capacity(expected);
            let mut input = result.into_stream();
            while let Some(bytes) = input
                .try_next()
                .await
                .map_err(|_| ArtifactError::Unavailable)?
            {
                if output
                    .len()
                    .checked_add(bytes.len())
                    .is_none_or(|n| n > expected)
                {
                    return Err(ArtifactError::Corrupt);
                }
                output.extend_from_slice(&bytes);
            }
            if output.len() != expected || sha256(&output) != expected_sha256 {
                return Err(ArtifactError::Corrupt);
            }
            Ok(Bytes::from(output))
        })
        .await
    }
    pub async fn delete(&self, relative: &str, deadline: Instant) -> Result<()> {
        let key = self.key(relative)?;
        let canceled = AtomicBool::new(false);
        bounded(deadline, &canceled, async {
            match self.object.delete(&key).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(_) => Err(ArtifactError::Unavailable),
            }
        })
        .await
    }
}
async fn bounded<T>(
    deadline: Instant,
    canceled: &AtomicBool,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    if canceled.load(Ordering::Acquire) {
        return Err(ArtifactError::Canceled);
    }
    let now = Instant::now();
    if deadline <= now {
        return Err(ArtifactError::TimedOut);
    }
    let deadline = deadline.min(now + IO_TIMEOUT);
    let canceled = async {
        loop {
            if canceled.load(Ordering::Acquire) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    timeout_at(deadline, async {
        tokio::select! {
            biased;
            _ = canceled => Err(ArtifactError::Canceled),
            result = operation => result,
        }
    })
    .await
    .map_err(|_| ArtifactError::TimedOut)?
}
pub(crate) fn sha256(bytes: &[u8]) -> String {
    use std::fmt::Write;
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

struct CappedWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.maximum)
        {
            return Err(io::Error::other("artifact byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) struct EncodedChunk {
    pub bytes: Bytes,
    pub rows: u64,
    pub sha256: String,
}
/// Bounded batching reduces PUT count for narrow results. A flush may retain one
/// newly received batch while encoding the previous bounded group.
pub(crate) struct ChunkEncoder {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    raw_bytes: usize,
    rows: u64,
    emitted: bool,
}
impl ChunkEncoder {
    pub fn new(schema: SchemaRef) -> Result<Self> {
        let mut writer = CappedWriter {
            bytes: Vec::new(),
            maximum: MAX_SCHEMA_BYTES,
        };
        serde_json::to_writer(&mut writer, schema.as_ref()).map_err(|_| ArtifactError::TooLarge)?;
        Ok(Self {
            schema,
            batches: Vec::new(),
            raw_bytes: 0,
            rows: 0,
            emitted: false,
        })
    }
    pub fn push(&mut self, batch: RecordBatch) -> Result<Option<EncodedChunk>> {
        if batch.schema() != self.schema {
            return Err(ArtifactError::Encoding);
        }
        let bytes = batch.get_array_memory_size();
        if bytes > MAX_BATCH_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        let flush = !self.batches.is_empty()
            && (self.raw_bytes.saturating_add(bytes) > MAX_BATCH_BYTES
                || self.batches.len() == MAX_BUFFERED_BATCHES);
        let output = if flush { Some(self.encode()?) } else { None };
        self.raw_bytes += bytes;
        self.rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or(ArtifactError::TooLarge)?;
        self.batches.push(batch);
        Ok(output)
    }
    pub fn finish(&mut self) -> Result<Option<EncodedChunk>> {
        if self.batches.is_empty() && self.emitted {
            return Ok(None);
        }
        self.encode().map(Some)
    }
    fn encode(&mut self) -> Result<EncodedChunk> {
        let output = CappedWriter {
            bytes: Vec::new(),
            maximum: MAX_CHUNK_BYTES,
        };
        let mut writer =
            StreamWriter::try_new(output, &self.schema).map_err(|_| ArtifactError::Encoding)?;
        for batch in &self.batches {
            writer.write(batch).map_err(|_| ArtifactError::Encoding)?;
        }
        writer.finish().map_err(|_| ArtifactError::Encoding)?;
        let output = writer
            .into_inner()
            .map_err(|_| ArtifactError::Encoding)?
            .bytes;
        let rows = self.rows;
        self.batches.clear();
        self.raw_bytes = 0;
        self.rows = 0;
        self.emitted = true;
        let digest = sha256(&output);
        Ok(EncodedChunk {
            bytes: Bytes::from(output),
            rows,
            sha256: digest,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestChunk {
    pub chunk_id: u32,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub version: u32,
    pub request_id: uuid::Uuid,
    pub generation: uuid::Uuid,
    pub format: String,
    pub complete: bool,
    pub schema: arrow_schema::Schema,
    pub rows: u64,
    pub bytes: u64,
    pub expires_at: String,
    pub chunks: Vec<ManifestChunk>,
}
impl Manifest {
    pub fn encode(&self) -> Result<Bytes> {
        if self.version != 1
            || self.format != "arrow_ipc_chunks"
            || !self.complete
            || self.chunks.is_empty()
            || self.chunks.len() > MAX_CHUNKS
        {
            return Err(ArtifactError::Corrupt);
        }
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for (sequence, chunk) in self.chunks.iter().enumerate() {
            if chunk.chunk_id as usize != sequence
                || chunk.bytes > MAX_CHUNK_BYTES as u64
                || chunk.sha256.len() != 64
                || !chunk
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(ArtifactError::Corrupt);
            }
            rows = rows
                .checked_add(chunk.rows)
                .ok_or(ArtifactError::TooLarge)?;
            bytes = bytes
                .checked_add(chunk.bytes)
                .ok_or(ArtifactError::TooLarge)?;
        }
        if rows != self.rows || bytes != self.bytes {
            return Err(ArtifactError::Corrupt);
        }
        let mut writer = CappedWriter {
            bytes: Vec::new(),
            maximum: MAX_MANIFEST_BYTES,
        };
        serde_json::to_writer(&mut writer, self).map_err(|_| ArtifactError::TooLarge)?;
        Ok(Bytes::from(writer.bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use std::{collections::BTreeMap, io::Cursor};

    fn settings() -> BTreeMap<String, String> {
        [
            ("DUCKFLEET_RESULT_DATA_PATH", "s3://warehouse/results/"),
            ("DUCKFLEET_RESULT_LAKE_DATA_PATH", "s3://warehouse/lake/"),
            ("DUCKFLEET_RESULT_AUTH_MODE", "static"),
            ("DUCKFLEET_RESULT_ACCESS_KEY", "test-key"),
            ("DUCKFLEET_RESULT_SECRET_KEY", "secret-sentinel"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
    }
    fn configured(values: &BTreeMap<String, String>) -> anyhow::Result<Option<StoreConfig>> {
        StoreConfig::from_lookup(|name| Ok(values.get(name).cloned()))
    }

    #[test]
    fn credentials_are_explicit_and_never_fall_back_to_ambient_aws_settings() {
        let complete = settings();
        assert!(configured(&complete).unwrap().is_some());
        for missing in [
            "DUCKFLEET_RESULT_AUTH_MODE",
            "DUCKFLEET_RESULT_ACCESS_KEY",
            "DUCKFLEET_RESULT_SECRET_KEY",
        ] {
            let mut values = complete.clone();
            values.remove(missing);
            values.insert("AWS_ACCESS_KEY_ID".into(), "ambient-key".into());
            values.insert("AWS_SECRET_ACCESS_KEY".into(), "ambient-secret".into());
            let error = configured(&values)
                .err()
                .expect("missing explicit auth accepted");
            assert!(!error.to_string().contains("secret-sentinel"));
            assert!(error.to_string().contains(missing));
        }
        let mut values = complete.clone();
        values.insert("DUCKFLEET_RESULT_AUTH_MODE".into(), "pod_identity".into());
        assert!(configured(&values).is_err());
        values.insert(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI".into(),
            "http://169.254.170.23/v1/credentials".into(),
        );
        assert!(configured(&values).is_err(), "token file is required");
        values.insert(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE".into(),
            "/var/run/pod-token".into(),
        );
        assert!(matches!(
            configured(&values).unwrap().unwrap().authentication,
            Authentication::PodIdentity { .. }
        ));
        for uri in [
            "http://169.254.169.254/latest/meta-data/",
            "http://example.com/",
            "http://169.254.170.23.example.com/",
            "http://user:pass@169.254.170.23/",
        ] {
            values.insert("AWS_CONTAINER_CREDENTIALS_FULL_URI".into(), uri.into());
            assert!(configured(&values).is_err(), "accepted {uri}");
        }
        values.remove("DUCKFLEET_RESULT_DATA_PATH");
        assert!(
            configured(&values).unwrap().is_none(),
            "disabled feature requires no credentials"
        );
    }

    #[test]
    fn paths_and_endpoints_cannot_overlap_lake_or_embed_credentials() {
        let mut values = settings();
        for path in [
            "s3://warehouse/lake/",
            "s3://warehouse/lake/results/",
            "s3://warehouse/../results",
            "s3://warehouse/results//x",
            "s3://warehouse/results/%2e%2e",
            "https://warehouse/results",
        ] {
            values.insert("DUCKFLEET_RESULT_DATA_PATH".into(), path.into());
            assert!(configured(&values).is_err(), "accepted {path}");
        }
        values.insert(
            "DUCKFLEET_RESULT_DATA_PATH".into(),
            "s3://warehouse/results/".into(),
        );
        values.insert(
            "DUCKFLEET_RESULT_LAKE_DATA_PATH".into(),
            "s3://warehouse/results/lake/".into(),
        );
        assert!(configured(&values).is_err());
        values.insert(
            "DUCKFLEET_RESULT_LAKE_DATA_PATH".into(),
            "s3://another-bucket/results/".into(),
        );
        assert!(configured(&values).is_ok());
        for endpoint in [
            "http://localhost:9000",
            "https://user:secret@example.com",
            "https://example.com/path",
            "https://example.com/?secret=1",
        ] {
            values.insert("DUCKFLEET_RESULT_ENDPOINT".into(), endpoint.into());
            assert!(configured(&values).is_err(), "accepted {endpoint}");
        }
        values.insert(
            "DUCKFLEET_RESULT_ENDPOINT".into(),
            "http://localhost:9000".into(),
        );
        values.insert("DUCKFLEET_RESULT_ALLOW_HTTP".into(), "true".into());
        assert!(configured(&values).is_ok());
        values.insert("DUCKFLEET_RESULT_ALLOW_HTTP".into(), "yes".into());
        assert!(configured(&values).is_err());
    }

    #[test]
    fn each_chunk_is_a_complete_arrow_stream_and_empty_results_keep_the_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let mut encoder = ChunkEncoder::new(schema.clone()).unwrap();
        let mut chunks = Vec::new();
        for value in 0..=MAX_BUFFERED_BATCHES {
            let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(value as i64)]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
            if let Some(chunk) = encoder.push(batch).unwrap() {
                chunks.push(chunk);
            }
        }
        chunks.push(encoder.finish().unwrap().unwrap());
        assert!(encoder.finish().unwrap().is_none());
        assert_eq!(chunks.len(), 2);
        let mut values = Vec::<i64>::new();
        for chunk in chunks {
            assert!(chunk.bytes.len() <= MAX_CHUNK_BYTES);
            assert_eq!(sha256(&chunk.bytes), chunk.sha256);
            let reader = StreamReader::try_new(Cursor::new(chunk.bytes), None).unwrap();
            assert_eq!(reader.schema(), schema);
            let mut rows = 0;
            for batch in reader {
                let batch = batch.unwrap();
                rows += batch.num_rows() as u64;
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
            }
            assert_eq!(rows, chunk.rows);
        }
        assert_eq!(
            values,
            (0..=MAX_BUFFERED_BATCHES as i64).collect::<Vec<_>>()
        );
        let empty = ChunkEncoder::new(schema.clone())
            .unwrap()
            .finish()
            .unwrap()
            .unwrap();
        assert_eq!(empty.rows, 0);
        let mut reader = StreamReader::try_new(Cursor::new(empty.bytes), None).unwrap();
        assert_eq!(reader.schema(), schema);
        assert!(reader.next().is_none());
    }

    #[test]
    fn encoder_caps_large_values_schema_and_output_before_unbounded_growth() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let mut encoder = ChunkEncoder::new(schema.clone()).unwrap();
        let array: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(MAX_BATCH_BYTES + 1)]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        assert!(matches!(encoder.push(batch), Err(ArtifactError::TooLarge)));
        let huge_schema = Arc::new(Schema::new(vec![Field::new(
            "x".repeat(MAX_SCHEMA_BYTES),
            DataType::Int64,
            false,
        )]));
        assert!(matches!(
            ChunkEncoder::new(huge_schema),
            Err(ArtifactError::TooLarge)
        ));
        let mut output = CappedWriter {
            bytes: Vec::new(),
            maximum: 4,
        };
        output.write_all(b"1234").unwrap();
        assert!(output.write_all(b"5").is_err());
        assert_eq!(output.bytes, b"1234");
    }

    #[tokio::test]
    async fn object_paths_lengths_and_digests_are_checked_before_delivery() {
        let store = Store {
            object: Arc::new(object_store::memory::InMemory::new()),
            prefix: "results".into(),
        };
        let canceled = AtomicBool::new(false);
        let bytes = Bytes::from_static(b"private-result");
        let digest = sha256(&bytes);
        let deadline = Instant::now() + Duration::from_secs(30);
        store
            .put(
                "job/generation/0.arrow",
                bytes.clone(),
                &digest,
                deadline,
                &canceled,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .get(
                    "job/generation/0.arrow",
                    bytes.len() as u64,
                    &digest,
                    MAX_CHUNK_BYTES,
                    deadline,
                    &canceled
                )
                .await
                .unwrap(),
            bytes
        );
        for path in [
            "../lake/x",
            "/outside",
            "x//y",
            "x/%2e%2e/y",
            "s3://other/x",
            "x\\y",
        ] {
            assert_eq!(
                store.delete(path, deadline).await,
                Err(ArtifactError::Corrupt)
            );
        }
        assert_eq!(
            store
                .get(
                    "job/generation/0.arrow",
                    bytes.len() as u64 + 1,
                    &digest,
                    MAX_CHUNK_BYTES,
                    deadline,
                    &canceled
                )
                .await,
            Err(ArtifactError::Corrupt)
        );
        assert_eq!(
            store
                .get(
                    "job/generation/0.arrow",
                    bytes.len() as u64,
                    &"0".repeat(64),
                    MAX_CHUNK_BYTES,
                    deadline,
                    &canceled
                )
                .await,
            Err(ArtifactError::Corrupt)
        );
        assert_eq!(
            store
                .get(
                    "not-written",
                    MAX_CHUNK_BYTES as u64 + 1,
                    &digest,
                    MAX_CHUNK_BYTES,
                    deadline,
                    &canceled
                )
                .await,
            Err(ArtifactError::TooLarge)
        );
        assert_eq!(
            store
                .put("not-written", bytes, &"0".repeat(64), deadline, &canceled)
                .await,
            Err(ArtifactError::Corrupt)
        );
        store
            .delete("job/generation/0.arrow", deadline)
            .await
            .unwrap();
        store
            .delete("job/generation/0.arrow", deadline)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn io_deadlines_and_cancellation_cover_the_whole_operation() {
        let canceled = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let result = bounded(Instant::now(), &canceled, async {
            started.store(true, Ordering::Release);
            Ok(())
        })
        .await;
        assert_eq!(result, Err(ArtifactError::TimedOut));
        assert!(
            !started.load(Ordering::Acquire),
            "expired IO must not begin"
        );
        let start = Instant::now();
        let result = bounded(
            start + Duration::from_secs(90),
            &canceled,
            std::future::pending::<Result<()>>(),
        )
        .await;
        assert_eq!(result, Err(ArtifactError::TimedOut));
        assert_eq!(Instant::now() - start, IO_TIMEOUT);
        let flag = canceled.clone();
        let pending = tokio::spawn(async move {
            bounded(
                Instant::now() + Duration::from_secs(90),
                &flag,
                std::future::pending::<Result<()>>(),
            )
            .await
        });
        tokio::task::yield_now().await;
        canceled.store(true, Ordering::Release);
        tokio::time::advance(Duration::from_millis(25)).await;
        assert_eq!(pending.await.unwrap(), Err(ArtifactError::Canceled));
    }

    #[test]
    fn manifests_require_contiguous_complete_chunks_and_exact_totals() {
        let mut manifest = Manifest {
            version: 1,
            request_id: uuid::Uuid::new_v4(),
            generation: uuid::Uuid::new_v4(),
            format: "arrow_ipc_chunks".into(),
            complete: true,
            schema: Schema::empty(),
            rows: 2,
            bytes: 32,
            expires_at: "2026-01-01T00:00:00Z".into(),
            chunks: vec![ManifestChunk {
                chunk_id: 0,
                rows: 2,
                bytes: 32,
                sha256: "a".repeat(64),
            }],
        };
        assert!(manifest.encode().is_ok());
        manifest.chunks[0].chunk_id = 1;
        assert_eq!(manifest.encode(), Err(ArtifactError::Corrupt));
        manifest.chunks[0].chunk_id = 0;
        manifest.rows = 3;
        assert_eq!(manifest.encode(), Err(ArtifactError::Corrupt));
        manifest.rows = 2;
        manifest.complete = false;
        assert_eq!(manifest.encode(), Err(ArtifactError::Corrupt));
    }
}
