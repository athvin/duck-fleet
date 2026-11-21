//! Bounded standalone job input and policy. Retained options deliberately exclude
//! SQL, parameters, and application context; replay hashes the caller's payload.
use crate::{ApiError, compute::ComputeRequest, failure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const MAX_JOB_PAYLOAD_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_JOB_QUEUE_TIMEOUT: u64 = 3600;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResultFormat {
    #[default]
    ArrowIpcChunks,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobLimits {
    pub runtime_seconds: u64,
    pub result_bytes: u64,
    pub result_rows: u64,
    pub retention_seconds: u64,
}
impl JobLimits {
    pub const HARD: Self = Self {
        runtime_seconds: 3600,
        result_bytes: 1024 * 1024 * 1024,
        result_rows: 10_000_000,
        retention_seconds: 24 * 3600,
    };
    pub const BASELINE: Self = Self {
        runtime_seconds: 60,
        retention_seconds: 3600,
        ..Self::HARD
    };

    pub fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.runtime_seconds > 0
                && self.result_bytes > 0
                && self.result_rows > 0
                && self.retention_seconds > 0
                && Self::HARD.covers(self),
            "job limits must be positive and at most 3600 seconds, 1 GiB, 10000000 rows, and 86400 retention seconds"
        );
        Ok(())
    }

    pub fn covers(self, other: Self) -> bool {
        self.runtime_seconds >= other.runtime_seconds
            && self.result_bytes >= other.result_bytes
            && self.result_rows >= other.result_rows
            && self.retention_seconds >= other.retention_seconds
    }

    pub fn intersect(self, other: Self) -> Self {
        Self {
            runtime_seconds: self.runtime_seconds.min(other.runtime_seconds),
            result_bytes: self.result_bytes.min(other.result_bytes),
            result_rows: self.result_rows.min(other.result_rows),
            retention_seconds: self.retention_seconds.min(other.retention_seconds),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptedJobPolicy {
    pub limits: JobLimits,
    pub queue_timeout_seconds: u64,
    pub format: ResultFormat,
    pub policy_generation: i64,
}
impl AcceptedJobPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.limits.validate()?;
        anyhow::ensure!(
            (1..=MAX_JOB_QUEUE_TIMEOUT).contains(&self.queue_timeout_seconds)
                && self.policy_generation >= 0,
            "invalid accepted job policy"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobPoolPolicy {
    pub ceilings: JobLimits,
    pub generation: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobDefaults {
    pub limits: JobLimits,
    pub queue_timeout_seconds: u64,
    pub format: ResultFormat,
}
impl Default for JobDefaults {
    fn default() -> Self {
        Self {
            limits: JobLimits {
                runtime_seconds: 300,
                retention_seconds: 3600,
                ..JobLimits::HARD
            },
            queue_timeout_seconds: 120,
            format: ResultFormat::ArrowIpcChunks,
        }
    }
}
impl JobDefaults {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.limits.validate()?;
        anyhow::ensure!(
            (1..=MAX_JOB_QUEUE_TIMEOUT).contains(&self.queue_timeout_seconds),
            "job default queue timeout must be 1–3600 seconds"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct JobResultRequest {
    pub format: Option<ResultFormat>,
    pub retention_seconds: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_rows: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalComputeIntent {
    pub mode: Option<crate::compute::Mode>,
    pub pool: Option<String>,
    pub cpu: Option<u16>,
    pub memory_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
}

/// Durable semantic options only. Never add SQL, bound values, context, or a full
/// JobRequest here: the private request digest is sufficient after retention.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobIntentOptions {
    pub compute: CanonicalComputeIntent,
    pub timeout_seconds: Option<u64>,
    pub queue_timeout_seconds: Option<u64>,
    pub result: JobResultRequest,
}
pub(crate) type RetainedJobOptions = JobIntentOptions;

impl JobIntentOptions {
    /// Resolve once, within one complete grant envelope. Explicit values never
    /// clamp; only omitted options inherit defaults clipped to this ceiling.
    pub fn resolve(
        &self,
        defaults: &JobDefaults,
        ceiling: JobLimits,
        generation: i64,
    ) -> Result<AcceptedJobPolicy, ApiError> {
        defaults.validate().map_err(|_| ApiError::internal())?;
        ceiling.validate().map_err(|_| ApiError::internal())?;
        if generation < 0 {
            return Err(ApiError::internal());
        }
        let limited = |value: Option<u64>, default: u64, cap: u64| match value {
            Some(value) if value == 0 || value > cap => Err(ApiError::new(
                failure::FORBIDDEN,
                "requested job bounds exceed the authorized envelope",
            )),
            Some(value) => Ok(value),
            None => Ok(default.min(cap)),
        };
        let policy = AcceptedJobPolicy {
            limits: JobLimits {
                runtime_seconds: limited(
                    self.timeout_seconds,
                    defaults.limits.runtime_seconds,
                    ceiling.runtime_seconds,
                )?,
                result_bytes: limited(
                    self.result.max_bytes,
                    defaults.limits.result_bytes,
                    ceiling.result_bytes,
                )?,
                result_rows: limited(
                    self.result.max_rows,
                    defaults.limits.result_rows,
                    ceiling.result_rows,
                )?,
                retention_seconds: limited(
                    self.result.retention_seconds,
                    defaults.limits.retention_seconds,
                    ceiling.retention_seconds,
                )?,
            },
            queue_timeout_seconds: self
                .queue_timeout_seconds
                .unwrap_or(defaults.queue_timeout_seconds),
            format: self.result.format.unwrap_or(defaults.format),
            policy_generation: generation,
        };
        policy.validate().map_err(|_| ApiError::internal())?;
        Ok(policy)
    }
}

fn empty_context() -> Value {
    json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobRequest {
    pub request_id: Uuid,
    #[serde(default)]
    pub retry_of: Option<Uuid>,
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

/// Not serializable: only active query_payloads receive the full request, never
/// a blanket serialization of this wrapper into a retained job/options record.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedJobRequest {
    request: JobRequest,
    options: RetainedJobOptions,
}
impl ValidatedJobRequest {
    pub fn request(&self) -> &JobRequest {
        &self.request
    }
    pub fn options(&self) -> &RetainedJobOptions {
        &self.options
    }
}
impl JobRequest {
    pub fn validate(self) -> Result<ValidatedJobRequest, ApiError> {
        if self.sql.trim().is_empty() || self.sql.len() > 262_144 {
            return Err(ApiError::bad("job SQL must contain 1–262144 bytes"));
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
                "job parameters must be at most 10000 scalar JSON values; context must be an object of at most 8192 bytes",
            ));
        }
        self.compute.validate()?;
        let result = self.result.clone().unwrap_or_default();
        for (value, maximum) in [
            (self.timeout_seconds, JobLimits::HARD.runtime_seconds),
            (self.queue_timeout_seconds, MAX_JOB_QUEUE_TIMEOUT),
            (result.retention_seconds, JobLimits::HARD.retention_seconds),
            (result.max_bytes, JobLimits::HARD.result_bytes),
            (result.max_rows, JobLimits::HARD.result_rows),
        ] {
            if value.is_some_and(|n| n == 0 || n > maximum) {
                return Err(ApiError::bad(
                    "job options must be positive and within the supported runtime, queue, result, and retention limits",
                ));
            }
        }
        if serde_json::to_vec(&self)
            .map_err(|_| ApiError::internal())?
            .len()
            > MAX_JOB_PAYLOAD_BYTES
        {
            return Err(ApiError::bad("job payload exceeds one MiB"));
        }
        let options = JobIntentOptions {
            compute: serde_json::from_value(self.compute.canonical_intent()?)
                .map_err(|_| ApiError::internal())?,
            timeout_seconds: self.timeout_seconds,
            queue_timeout_seconds: self.queue_timeout_seconds,
            result,
        };
        Ok(ValidatedJobRequest {
            request: self,
            options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(extra: Value) -> JobRequest {
        let mut value = json!({"request_id":Uuid::new_v4(),"sql":"SELECT 1"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn retained_options_never_copy_statement_parameters_or_context() {
        let validated = request(json!({"sql":"SELECT 'secret-sql'", "parameters":["secret-bind"],"context":{"model":"secret-context"}})).validate().unwrap();
        let encoded = serde_json::to_string(validated.options()).unwrap();
        for secret in [
            "secret-sql",
            "secret-bind",
            "secret-context",
            "\"sql\"",
            "\"parameters\"",
            "\"context\"",
        ] {
            assert!(!encoded.contains(secret));
        }
        let mut injected = serde_json::to_value(validated.options()).unwrap();
        injected["sql"] = json!("hidden copy");
        assert!(serde_json::from_value::<RetainedJobOptions>(injected).is_err());
    }

    #[test]
    fn option_normalization_preserves_omission_without_losing_equivalent_quantities() {
        let omitted = request(json!({})).validate().unwrap();
        let empty = request(json!({"compute":{},"timeout_seconds":null,"result":{}}))
            .validate()
            .unwrap();
        assert_eq!(omitted.options(), empty.options());
        let explicit = request(json!({"timeout_seconds":60})).validate().unwrap();
        assert_ne!(omitted.options(), explicit.options());
        let a = request(json!({"compute":{"pool":"batch","memory":"1Gi"}}))
            .validate()
            .unwrap();
        let b = request(json!({"compute":{"pool":"batch","memory":"1024Mi"}}))
            .validate()
            .unwrap();
        assert_eq!(a.options(), b.options());
    }

    #[test]
    fn omitted_defaults_clip_but_explicit_limits_refuse() {
        let defaults = JobDefaults::default();
        let options = request(json!({})).validate().unwrap();
        let accepted = options
            .options()
            .resolve(&defaults, JobLimits::BASELINE, 7)
            .unwrap();
        assert_eq!(accepted.limits.runtime_seconds, 60);
        assert_eq!(accepted.policy_generation, 7);
        assert_eq!(accepted.queue_timeout_seconds, 120);
        let options = request(json!({"timeout_seconds":300})).validate().unwrap();
        assert_eq!(
            options
                .options()
                .resolve(&defaults, JobLimits::BASELINE, 7)
                .unwrap_err()
                .0
                .kind,
            "forbidden"
        );
        assert_eq!(
            options
                .options()
                .resolve(&defaults, JobLimits::HARD, 8)
                .unwrap()
                .limits
                .runtime_seconds,
            300
        );
    }

    #[test]
    fn job_validation_is_independent_of_interactive_deadlines_and_checks_bounds() {
        for seconds in [1, 60, 600, 3600] {
            assert!(
                request(json!({"timeout_seconds":seconds}))
                    .validate()
                    .is_ok()
            );
        }
        for extra in [
            json!({"timeout_seconds":0}),
            json!({"timeout_seconds":3601}),
            json!({"queue_timeout_seconds":3601}),
            json!({"result":{"max_rows":0}}),
            json!({"result":{"max_bytes":u64::MAX}}),
            json!({"parameters":[{}]}),
        ] {
            assert!(request(extra).validate().is_err());
        }
        for extra in [
            json!({"result":{"format":"parquet"}}),
            json!({"session_id":"forbidden"}),
            json!({"result":{"path":"s3://arbitrary"}}),
        ] {
            let mut value = json!({"request_id":Uuid::new_v4(),"sql":"SELECT 1"});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(serde_json::from_value::<JobRequest>(value).is_err());
        }
    }
}
