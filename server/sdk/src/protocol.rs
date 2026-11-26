//! Wire types shared with the gateway: compute selection, durable-job requests,
//! and the verified result manifest. They mirror the server's own contracts;
//! the server's tests deserialize these encodings to keep them from diverging.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug)]
pub struct Failure {
    pub kind: &'static str,
    pub retry_safe: bool,
}

fn invalid(message: impl Into<String>) -> (Failure, String) {
    (
        Failure {
            kind: "invalid_request",
            retry_safe: true,
        },
        message.into(),
    )
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Persistent,
    Isolated,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk: Option<String>,
}

impl ComputeRequest {
    pub fn validate(&self) -> Result<(), (Failure, String)> {
        if self.cpu == Some(0) {
            return Err(invalid("cpu must be at least 1"));
        }
        if let Some(pool) = &self.pool {
            let bytes = pool.as_bytes();
            if bytes.is_empty()
                || bytes.len() > 40
                || !bytes[0].is_ascii_lowercase()
                || !bytes[bytes.len() - 1].is_ascii_alphanumeric()
                || !bytes
                    .iter()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            {
                return Err(invalid("invalid pool name"));
            }
        }
        if matches!(self.mode, Some(Mode::Isolated)) && self.pool.is_some() {
            return Err(invalid("isolated compute cannot select a persistent pool"));
        }
        for (name, value, minimum) in [
            ("memory", &self.memory, 256 * 1024 * 1024),
            ("disk", &self.disk, 1024 * 1024 * 1024),
        ] {
            if let Some(value) = value {
                let valid = ["Ki", "Mi", "Gi"]
                    .iter()
                    .find_map(|suffix| {
                        value.strip_suffix(suffix).and_then(|number| {
                            let multiplier = match *suffix {
                                "Gi" => 1024 * 1024 * 1024,
                                "Mi" => 1024 * 1024,
                                _ => 1024,
                            };
                            number.trim().parse::<u64>().ok()?.checked_mul(multiplier)
                        })
                    })
                    .is_some_and(|bytes| bytes >= minimum);
                if !valid {
                    return Err(invalid(format!("invalid or too-small {name} quantity")));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultFormat {
    #[default]
    ArrowIpcChunks,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobResultRequest {
    pub format: Option<ResultFormat>,
    pub retention_seconds: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_rows: Option<u64>,
}

fn empty_context() -> Value {
    Value::Object(serde_json::Map::new())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    pub request_id: uuid::Uuid,
    #[serde(default)]
    pub retry_of: Option<uuid::Uuid>,
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<Value>,
    #[serde(default = "empty_context")]
    pub context: Value,
    #[serde(default)]
    pub compute: ComputeRequest,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub queue_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub result: Option<JobResultRequest>,
}

pub struct JobLimits;
impl JobLimits {
    pub const HARD: Limits = Limits {
        result_bytes: 1024 * 1024 * 1024,
        result_rows: 10_000_000,
    };
}
pub struct Limits {
    pub result_bytes: u64,
    pub result_rows: u64,
}

impl JobRequest {
    pub fn validate(self) -> Result<Self, (Failure, String)> {
        if self.sql.trim().is_empty() || self.sql.len() > 262_144 {
            return Err(invalid("job SQL must contain 1–262144 bytes"));
        }
        if !self.context.is_object()
            || self.context.to_string().len() > 8192
            || self.parameters.len() > 10_000
            || self
                .parameters
                .iter()
                .any(|value| value.is_array() || value.is_object())
        {
            return Err(invalid("invalid job parameters or context"));
        }
        self.compute.validate()?;
        let result = self.result.clone().unwrap_or_default();
        for (value, maximum) in [
            (self.timeout_seconds, 3600),
            (self.queue_timeout_seconds, 3600),
            (result.retention_seconds, 86400),
            (result.max_bytes, JobLimits::HARD.result_bytes),
            (result.max_rows, JobLimits::HARD.result_rows),
        ] {
            if value.is_some_and(|number| number == 0 || number > maximum) {
                return Err(invalid("job option is outside its supported bounds"));
            }
        }
        if serde_json::to_vec(&self)
            .map_err(|_| invalid("cannot encode job request"))?
            .len()
            > 1024 * 1024
        {
            return Err(invalid("job payload exceeds one MiB"));
        }
        Ok(self)
    }
}

pub const DEFAULT_QUEUE_TIMEOUT: u64 = 10;
pub const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_CHUNKS: usize = 4096;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestChunk {
    pub chunk_id: u32,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
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
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        if self.version != 1
            || self.format != "arrow_ipc_chunks"
            || !self.complete
            || self.chunks.is_empty()
            || self.chunks.len() > MAX_CHUNKS
        {
            return Err("invalid manifest".into());
        }
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for (sequence, chunk) in self.chunks.iter().enumerate() {
            if chunk.chunk_id as usize != sequence
                || chunk.bytes > MAX_CHUNK_BYTES as u64
                || chunk.sha256.len() != 64
                || !chunk
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err("invalid manifest chunk".into());
            }
            rows = rows
                .checked_add(chunk.rows)
                .ok_or("manifest row overflow")?;
            bytes = bytes
                .checked_add(chunk.bytes)
                .ok_or("manifest byte overflow")?;
        }
        if rows != self.rows || bytes != self.bytes {
            return Err("manifest totals do not match its chunks".into());
        }
        let encoded = serde_json::to_vec(self).map_err(|_| "cannot encode manifest".to_owned())?;
        if encoded.len() > MAX_MANIFEST_BYTES {
            return Err("manifest exceeds its size bound".into());
        }
        Ok(encoded)
    }
}
