//! The two resource classes provisioned by the local Docker harness.
use crate::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Where a statement runs. Named explicitly rather than inferred, so a caller that
/// needs isolation says so and is refused if it cannot be provided.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A prestarted worker, shared with other sessions of the same class.
    #[default]
    Persistent,
    /// A worker created for this session alone and discarded afterwards. Accepted by
    /// the contract and not yet runnable; see the `COMPUTE_UNAVAILABLE` failure.
    Isolated,
}
impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Isolated => "isolated",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputeRequest {
    /// Kept as text on the wire and parsed by `mode`, deliberately. Deserializing
    /// straight into the enum would hand an unknown value to the JSON extractor,
    /// whose rejection is an unclassified 422 with no `failure_kind` — the one
    /// failure in this API a client could not act on programmatically.
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
    Mode::Persistent.name().into()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Size {
    Small,
    Medium,
}
/// Where a resolved request will run. Both halves are reported to the caller and
/// recorded in history, so a placement can be compared against what was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Placement {
    pub mode: Mode,
    pub size: Size,
}

impl ComputeRequest {
    /// Parses the requested mode, naming the modes that exist when it is unknown.
    fn mode(&self) -> Result<Mode, ApiError> {
        match self.compute.as_str() {
            "persistent" => Ok(Mode::Persistent),
            "isolated" => Ok(Mode::Isolated),
            other => Err(ApiError::bad(format!(
                "unknown compute mode {other:?}; expected persistent or isolated"
            ))),
        }
    }

    /// Resolves a request to a placement, or refuses it.
    ///
    /// Checked in the order the caller must fix things: an unknown mode and a
    /// malformed resource spec are contract violations, an unavailable mode is a
    /// capability limit that bounds which sizes are even possible, and the size is
    /// checked against what is actually provisioned last. There is no fallback
    /// between modes — a request for isolation is never served by shared compute,
    /// because quietly doing so would void the guarantee the caller asked for.
    pub fn resolve(&self) -> Result<Placement, ApiError> {
        let mode = self.mode()?;
        if self.cpu.is_some() != self.memory.is_some() {
            return Err(ApiError::bad(
                "cpu and memory must be given together, or both omitted for the default",
            ));
        }
        if mode == Mode::Isolated {
            return Err(ApiError::new(
                crate::failure::COMPUTE_UNAVAILABLE,
                "isolated compute is not available in this deployment; no statement ran \
                 and the request was not downgraded to shared compute",
            ));
        }
        let size = match (self.cpu, self.memory.as_deref()) {
            (None, None) | (Some(1), Some("1Gi")) => Size::Small,
            (Some(2), Some("2Gi")) => Size::Medium,
            _ => {
                return Err(ApiError::bad(
                    "unsupported compute request: specify cpu=1, memory=1Gi or cpu=2, memory=2Gi together",
                ));
            }
        };
        Ok(Placement { mode, size })
    }
}

impl Placement {
    /// Billable size comes from what the caller requested, not from the worker it
    /// landed on. A persistent worker is shared by up to 32 sessions, so charging
    /// for the provision would bill each one for the whole worker.
    pub fn units(self) -> i64 {
        crate::pricing::normalized_units(self.size.cpu(), self.size.memory_bytes())
    }
    pub fn describe(self) -> Value {
        let mut value = self.size.placement();
        value["compute"] = json!(self.mode.name());
        value
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
    pub fn validate(&self) -> Result<Placement, ApiError> {
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
    fn request(value: Value) -> ComputeRequest {
        serde_json::from_value(value).expect("should deserialize")
    }

    #[test]
    fn exact_sizes_and_default_resolve() {
        let placement = SessionRequest::default().validate().unwrap();
        assert_eq!(
            (placement.mode, placement.size),
            (Mode::Persistent, Size::Small)
        );
        assert_eq!(Size::Medium.request().resolve().unwrap().size, Size::Medium);
        let session: SessionRequest = serde_json::from_value(json!({"compute":{}})).unwrap();
        assert_eq!(session.validate().unwrap().size, Size::Small);
    }

    #[test]
    fn unsupported_or_incomplete_requests_never_fall_back() {
        for value in [
            json!({"cpu":32,"memory":"128Gi"}),
            json!({"cpu":2}),
            json!({"memory":"2Gi"}),
            json!({"cpu":1,"memory":"2Gi"}),
        ] {
            assert!(request(value.clone()).resolve().is_err(), "{value}");
        }
        // An unknown mode is a classified request error, not an extractor rejection:
        // the client gets a failure_kind it can act on and a message naming the
        // modes that exist.
        for value in [json!({"compute":"burst"}), json!({"compute":"Persistent"})] {
            let error = request(value.clone()).resolve().unwrap_err();
            assert_eq!(
                error.0.kind,
                crate::failure::INVALID_REQUEST.kind,
                "{value}"
            );
            assert!(error.1.contains("persistent or isolated"), "{}", error.1);
        }
        assert!(
            serde_json::from_value::<ComputeRequest>(json!({"cpu":1,"memory":"1Gi","gpu":1}))
                .is_err()
        );
    }

    /// Isolation is a guarantee, so a request for it is refused rather than served
    /// by shared compute. The code is distinguishable from a malformed request: one
    /// means ask differently, the other means ask later.
    #[test]
    fn isolated_is_refused_and_never_downgraded() {
        let error = request(json!({"compute":"isolated","cpu":1,"memory":"1Gi"}))
            .resolve()
            .unwrap_err();
        assert_eq!(error.0, crate::failure::COMPUTE_UNAVAILABLE);
        assert_ne!(error.0.kind, crate::failure::INVALID_REQUEST.kind);
        // Nothing ran, so the caller may legitimately send it again once supported.
        assert!(error.0.retry_safe);
        assert!(error.1.contains("not downgraded"), "{}", error.1);
    }

    #[test]
    fn a_partial_resource_spec_is_reported_before_mode_availability() {
        // Malformed is malformed regardless of mode, so that is what the caller hears.
        let error = request(json!({"compute":"isolated","cpu":1}))
            .resolve()
            .unwrap_err();
        assert_eq!(error.0.kind, crate::failure::INVALID_REQUEST.kind);
        assert!(error.1.contains("together"), "{}", error.1);
    }

    #[test]
    fn placement_reports_the_resolved_mode_to_the_caller() {
        let described = Size::Medium.request().resolve().unwrap().describe();
        assert_eq!(described["compute"], json!("persistent"));
        assert_eq!(described["worker_id"], json!("medium"));
        assert_eq!(described["cpu"], json!(2));
    }

    /// A persistent worker is shared by up to 32 sessions, so charging for the
    /// provision would bill each one for the whole worker. Units follow the request.
    #[test]
    fn billable_units_follow_the_request() {
        for (value, expected) in [
            (json!({"cpu":1,"memory":"1Gi"}), 1),
            (json!({"cpu":2,"memory":"2Gi"}), 2),
        ] {
            let placement = request(value).resolve().unwrap();
            assert_eq!(placement.units(), expected);
            assert_eq!(
                placement.units(),
                crate::pricing::normalized_units(
                    placement.size.cpu(),
                    placement.size.memory_bytes()
                )
            );
        }
    }
}
