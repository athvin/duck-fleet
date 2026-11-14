//! The two resource classes provisioned by the local Docker harness.
use crate::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputeRequest {
    #[serde(default = "persistent")]
    pub compute: String,
    pub cpu: Option<u16>,
    pub memory: Option<String>,
}
impl Default for ComputeRequest {
    fn default() -> Self {
        Self {
            compute: persistent(),
            cpu: None,
            memory: None,
        }
    }
}
fn persistent() -> String {
    "persistent".into()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Size {
    Small,
    Medium,
}
impl ComputeRequest {
    pub fn resolve(&self) -> Result<Size, ApiError> {
        if self.compute != "persistent" {
            return Err(ApiError::bad(
                "only persistent Docker workers are supported",
            ));
        }
        match (self.cpu, self.memory.as_deref()) {
            (None, None) | (Some(1), Some("1Gi")) => Ok(Size::Small),
            (Some(2), Some("2Gi")) => Ok(Size::Medium),
            _ => Err(ApiError::bad(
                "unsupported compute request: specify cpu=1, memory=1Gi or cpu=2, memory=2Gi together",
            )),
        }
    }
}
impl Size {
    pub fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
        }
    }
    pub fn cpu(self) -> u16 {
        match self {
            Self::Small => 1,
            Self::Medium => 2,
        }
    }
    pub fn memory_bytes(self) -> u64 {
        u64::from(self.cpu()) * 1024 * 1024 * 1024
    }
    /// Normalized billable size of this class, used by metering and pricing.
    pub fn units(self) -> i64 {
        crate::pricing::normalized_units(self.cpu(), self.memory_bytes())
    }
    pub fn engine_memory(self) -> &'static str {
        match self {
            Self::Small => "512MiB",
            Self::Medium => "1GiB",
        }
    }
    pub fn request(self) -> ComputeRequest {
        ComputeRequest {
            compute: persistent(),
            cpu: Some(self.cpu()),
            memory: Some(format!("{}Gi", self.cpu())),
        }
    }
    pub fn placement(self) -> Value {
        json!({"worker_id":self.name(), "cpu":self.cpu(), "memory_bytes":self.memory_bytes(), "duckdb_memory_limit":self.engine_memory()})
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct SessionRequest {
    pub compute: ComputeRequest,
    pub context: Value,
}
impl Default for SessionRequest {
    fn default() -> Self {
        Self {
            compute: Size::Small.request(),
            context: json!({}),
        }
    }
}
impl SessionRequest {
    pub fn validate(&self) -> Result<Size, ApiError> {
        if !self.context.is_object() || self.context.to_string().len() > 8192 {
            return Err(ApiError::bad(
                "session context must be an object of at most 8192 bytes",
            ));
        }
        self.compute.resolve()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_sizes_and_default_resolve() {
        assert_eq!(SessionRequest::default().validate().ok(), Some(Size::Small));
        assert_eq!(Size::Medium.request().resolve().ok(), Some(Size::Medium));
        let request: SessionRequest = serde_json::from_value(json!({"compute":{}})).unwrap();
        assert_eq!(request.validate().ok(), Some(Size::Small));
    }
    #[test]
    fn unsupported_or_incomplete_requests_never_fall_back() {
        for value in [
            json!({"cpu":32,"memory":"128Gi"}),
            json!({"cpu":2}),
            json!({"memory":"2Gi"}),
            json!({"cpu":1,"memory":"2Gi"}),
            json!({"compute":"burst"}),
        ] {
            assert!(
                serde_json::from_value::<ComputeRequest>(value)
                    .unwrap()
                    .resolve()
                    .is_err()
            );
        }
        assert!(
            serde_json::from_value::<ComputeRequest>(json!({"cpu":1,"memory":"1Gi","gpu":1}))
                .is_err()
        );
    }
}
