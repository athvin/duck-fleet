//! Shared validated query input and operator admission policy.
use crate::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const DEFAULT_QUEUE_TIMEOUT: u64 = 10;
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Acceptance {
    Accepted,
    NotAccepted,
    Unknown,
}
impl Acceptance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::NotAccepted => "not_accepted",
            Self::Unknown => "unknown",
        }
    }
}
fn queue_timeout() -> u64 {
    DEFAULT_QUEUE_TIMEOUT
}
fn execution_timeout() -> u64 {
    30
}
fn context() -> Value {
    json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryRequest {
    pub request_id: Uuid,
    #[serde(default)]
    pub retry_of: Option<Uuid>,
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<Value>,
    #[serde(default = "context")]
    pub context: Value,
    #[serde(default = "execution_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "queue_timeout")]
    pub queue_timeout_seconds: u64,
}
impl QueryRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.sql.trim().is_empty()
            || self.sql.len() > 262_144
            || !(1..=60).contains(&self.timeout_seconds)
            || !(1..=120).contains(&self.queue_timeout_seconds)
        {
            return Err(ApiError::bad(
                "SQL must contain 1–262144 bytes; execution timeout 1–60 seconds; queue timeout 1–120 seconds",
            ));
        }
        if !self.context.is_object()
            || self.context.to_string().len() > 8192
            || self.parameters.len() > 10_000
            || self
                .parameters
                .iter()
                .any(|p| p.is_array() || p.is_object())
        {
            return Err(ApiError::bad(
                "parameters must be at most 10000 scalar JSON values; context must be an object of at most 8192 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Protocol {
    Http,
    Pgwire,
    Platform,
    Direct,
}
impl Protocol {
    pub fn name(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Pgwire => "pgwire",
            Self::Platform => "platform",
            Self::Direct => "direct",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdmissionLimits {
    pub max_queued_queries: u32,
    pub max_running_queries: u32,
    pub max_queued_per_principal: u32,
    pub max_running_per_principal: u32,
}
impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            max_queued_queries: 256,
            max_running_queries: 16,
            max_queued_per_principal: 16,
            max_running_per_principal: 16,
        }
    }
}
impl AdmissionLimits {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=65535).contains(&self.max_queued_queries)
                && (1..=1024).contains(&self.max_running_queries)
                && (1..=self.max_queued_queries).contains(&self.max_queued_per_principal)
                && (1..=self.max_running_queries).contains(&self.max_running_per_principal),
            "admission queued limits must be 1–65535, running limits 1–1024, and principal limits cannot exceed pool limits"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AdmissionLimitsOverride {
    pub max_queued_queries: Option<u32>,
    pub max_running_queries: Option<u32>,
    pub max_queued_per_principal: Option<u32>,
    pub max_running_per_principal: Option<u32>,
}
impl AdmissionLimitsOverride {
    pub fn resolve(&self, defaults: AdmissionLimits) -> anyhow::Result<AdmissionLimits> {
        let running = self
            .max_running_queries
            .unwrap_or(defaults.max_running_queries);
        let limits = AdmissionLimits {
            max_queued_queries: self
                .max_queued_queries
                .unwrap_or(defaults.max_queued_queries),
            max_running_queries: running,
            max_queued_per_principal: self
                .max_queued_per_principal
                .unwrap_or(defaults.max_queued_per_principal),
            max_running_per_principal: self.max_running_per_principal.unwrap_or(running),
        };
        limits.validate()?;
        Ok(limits)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkerLimits {
    pub max_queued_queries: u32,
    pub max_queued_bytes: u64,
}
impl WorkerLimits {
    pub fn from_env() -> anyhow::Result<Self> {
        let count = crate::configured_limit("DUCKFLEET_WORKER_MAX_QUEUED_QUERIES", 64, 4096)?;
        let bytes = crate::configured_limit(
            "DUCKFLEET_WORKER_MAX_QUEUED_BYTES",
            16 * 1024 * 1024,
            256 * 1024 * 1024,
        )?;
        anyhow::ensure!(
            bytes >= 1024 * 1024,
            "worker queued bytes must be at least one MiB"
        );
        Ok(Self {
            max_queued_queries: u32::try_from(count)?,
            max_queued_bytes: u64::try_from(bytes)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn execution_and_queue_deadlines_are_independent_and_bounded() {
        let id = Uuid::new_v4();
        let mut request: QueryRequest =
            serde_json::from_value(json!({"request_id":id,"sql":"SELECT 1"})).unwrap();
        assert_eq!(request.queue_timeout_seconds, 10);
        assert_eq!(request.timeout_seconds, 30);
        request.queue_timeout_seconds = 120;
        request.timeout_seconds = 1;
        assert!(request.validate().is_ok());
        for (queue, execution) in [(0, 30), (121, 30), (10, 0), (10, 61)] {
            request.queue_timeout_seconds = queue;
            request.timeout_seconds = execution;
            assert!(request.validate().is_err());
        }
    }
    #[test]
    fn principal_running_defaults_follow_pool_without_serializing_one_principal() {
        assert_eq!(AdmissionLimits::default().max_running_per_principal, 16);
        let overrides = AdmissionLimitsOverride {
            max_running_queries: Some(32),
            ..Default::default()
        };
        assert_eq!(
            overrides
                .resolve(AdmissionLimits::default())
                .unwrap()
                .max_running_per_principal,
            32
        );
        let limited = AdmissionLimitsOverride {
            max_running_queries: Some(32),
            max_running_per_principal: Some(1),
            ..Default::default()
        };
        assert_eq!(
            limited
                .resolve(AdmissionLimits::default())
                .unwrap()
                .max_running_per_principal,
            1
        );
        let invalid = AdmissionLimitsOverride {
            max_running_queries: Some(1),
            max_running_per_principal: Some(2),
            ..Default::default()
        };
        assert!(invalid.resolve(AdmissionLimits::default()).is_err());
    }
}
