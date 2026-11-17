//! What a caller may ask for, and what that resolves to.
//!
//! Resources are carried as values rather than named classes. The two-class enum
//! this replaced existed because Compose prestarted exactly two containers and the
//! gateway picked one by hostname; the request contract was always direct. Adding a
//! size is now running a worker with those limits rather than teaching the platform
//! a new name.
//!
//! A worker still declares what it is meant to be and checks its cgroup against
//! that declaration, so a container that did not get the limits the chart asked for
//! refuses to start instead of quietly serving a different size.
use crate::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
/// Below this the engine's half share is too small to load extensions and hold a
/// result, so it is refused rather than started and left to fail obscurely.
const MINIMUM_MEMORY: u64 = 256 * MIB;

/// Where a statement runs. Named explicitly rather than inferred, so a caller that
/// needs isolation says so and is refused if it cannot be provided.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A prestarted worker, shared with other sessions of the same resources.
    #[default]
    Persistent,
    /// A worker created for this session alone and discarded afterwards. Served
    /// only where the gateway holds the pod grant (see `isolated.rs`); refused
    /// with the `COMPUTE_UNAVAILABLE` failure everywhere else.
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

/// A compute size. Memory is the **container cap**, not the engine's share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Resources {
    pub cpu: u16,
    pub memory_bytes: u64,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            cpu: 1,
            memory_bytes: GIB,
        }
    }
}
impl Resources {
    /// Parses a size from text, applying the same rules a request is held to.
    ///
    /// Shared with the worker's own configuration so the two cannot disagree about
    /// what a valid size is: a worker configured with something a caller could never
    /// request would be unreachable, which is worse than refusing to start.
    pub fn parse(cpu: &str, memory: &str) -> Result<Self, String> {
        let cpu: u16 = cpu
            .trim()
            .parse()
            .map_err(|_| format!("cpu {cpu:?} must be a whole number"))?;
        let memory_bytes = parse_memory(memory.trim())
            .ok_or_else(|| format!("memory {memory:?} must be a binary quantity such as 2Gi"))?;
        let resources = Self { cpu, memory_bytes };
        resources.usable()?;
        Ok(resources)
    }
    /// Rejects a size too small to run an engine, whoever asked for it.
    fn usable(self) -> Result<(), String> {
        if self.cpu == 0 {
            return Err("cpu must be at least 1".into());
        }
        if self.memory_bytes < MINIMUM_MEMORY {
            return Err(format!(
                "memory must be at least {}Mi; the engine is held to half the cap",
                MINIMUM_MEMORY / MIB
            ));
        }
        Ok(())
    }
    /// DuckDB is held to half the container cap, leaving room for extensions,
    /// networking and Arrow results. This is the rule the README documents; it was
    /// previously a per-class constant and is the same arithmetic.
    pub fn engine_memory(self) -> String {
        format!("{}MiB", self.memory_bytes / 2 / MIB)
    }
    /// Normalized billable size, from the resources rather than from a class name.
    pub fn units(self) -> i64 {
        crate::pricing::normalized_units(self.cpu, self.memory_bytes)
    }
    pub fn memory(self) -> String {
        if self.memory_bytes.is_multiple_of(GIB) {
            format!("{}Gi", self.memory_bytes / GIB)
        } else {
            format!("{}Mi", self.memory_bytes / MIB)
        }
    }
    pub fn request(self) -> ComputeRequest {
        ComputeRequest {
            compute: persistent(),
            cpu: Some(self.cpu),
            memory: Some(self.memory()),
        }
    }
    pub fn describe(self) -> Value {
        json!({"cpu": self.cpu, "memory_bytes": self.memory_bytes})
    }
}

/// Parses a binary quantity such as `2Gi` or `512Mi`.
///
/// A suffix is required: a bare number would be ambiguous between bytes and
/// mebibytes, and both Kubernetes and Docker express a container cap with a suffix.
fn parse_memory(text: &str) -> Option<u64> {
    for (suffix, multiplier) in [("Gi", GIB), ("Mi", MIB), ("Ki", 1024)] {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.trim().parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    None
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

/// Where a resolved request will run. Both halves are reported to the caller and
/// recorded in history, so a placement can be compared against what was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Placement {
    pub mode: Mode,
    pub resources: Resources,
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
    /// Only the request itself is judged here. Whether any worker provides these
    /// resources — or whether this deployment can create a pod at all — is the
    /// router's question, and answering it here would turn a temporarily absent
    /// worker into a malformed request.
    pub fn resolve(&self) -> Result<Placement, ApiError> {
        let mode = self.mode()?;
        let resources = match (self.cpu, self.memory.as_deref()) {
            (None, None) => Resources::default(),
            (Some(cpu), Some(memory)) => {
                Resources::parse(&cpu.to_string(), memory).map_err(ApiError::bad)?
            }
            _ => {
                return Err(ApiError::bad(
                    "cpu and memory must be given together, or both omitted for the default",
                ));
            }
        };
        Ok(Placement { mode, resources })
    }
}

/// Refused because this deployment cannot run isolated compute, which most cannot:
/// the gateway needs RBAC to create pods and the deploy asserts it cannot grant
/// itself any. Defined here, beside the contract, so the one sentence that promises
/// no downgrade cannot drift from the mode it describes.
pub(crate) fn unavailable() -> ApiError {
    ApiError::new(
        crate::failure::COMPUTE_UNAVAILABLE,
        "isolated compute is not available in this deployment; no statement ran \
         and the request was not downgraded to shared compute",
    )
}

impl Placement {
    /// Billable size comes from what the caller requested, not from the worker it
    /// landed on. A persistent worker is shared by up to 32 sessions, so charging
    /// for the provision would bill each one for the whole worker.
    pub fn units(self) -> i64 {
        self.resources.units()
    }
    /// The resolved request, so the worker is told the mode as well as the size.
    ///
    /// Sending only the size would have an isolated session recorded as persistent
    /// in history, because a pod cannot tell from a request alone whether it was
    /// created for one caller or is shared with thirty others.
    pub fn request(self) -> ComputeRequest {
        ComputeRequest {
            compute: self.mode.name().into(),
            cpu: Some(self.resources.cpu),
            memory: Some(self.resources.memory()),
        }
    }
    /// `worker_id` is the worker's own identity, passed in rather than derived from
    /// its size: reservation recovery keys on it, so it must stay stable even if the
    /// worker is given different resources.
    pub fn describe(self, worker_id: &str) -> Value {
        let mut value = self.resources.describe();
        value["worker_id"] = json!(worker_id);
        value["compute"] = json!(self.mode.name());
        value
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
            compute: Resources::default().request(),
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
    fn resolved(value: Value) -> Resources {
        request(value).resolve().unwrap().resources
    }

    #[test]
    fn the_default_and_the_two_provisioned_sizes_resolve() {
        let placement = SessionRequest::default().validate().unwrap();
        assert_eq!(placement.mode, Mode::Persistent);
        assert_eq!(placement.resources, Resources::default());
        assert_eq!(
            resolved(json!({"cpu":1,"memory":"1Gi"})),
            Resources {
                cpu: 1,
                memory_bytes: GIB
            }
        );
        assert_eq!(
            resolved(json!({"cpu":2,"memory":"2Gi"})),
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB
            }
        );
    }

    /// The point of this change: a size nothing was taught about still resolves.
    /// Whether a worker provides it is the router's question, not this one's.
    #[test]
    fn a_size_outside_the_old_two_classes_resolves() {
        assert_eq!(
            resolved(json!({"cpu":8,"memory":"32Gi"})),
            Resources {
                cpu: 8,
                memory_bytes: 32 * GIB
            }
        );
        assert_eq!(
            resolved(json!({"cpu":1,"memory":"512Mi"})),
            Resources {
                cpu: 1,
                memory_bytes: 512 * MIB
            }
        );
    }

    #[test]
    fn incomplete_or_unusable_requests_are_refused() {
        for value in [
            json!({"cpu":2}),
            json!({"memory":"2Gi"}),
            json!({"cpu":0,"memory":"1Gi"}),
            json!({"cpu":1,"memory":"128Mi"}),
            json!({"cpu":1,"memory":"1GB"}),
            json!({"cpu":1,"memory":"1024"}),
            json!({"cpu":1,"memory":"lots"}),
        ] {
            assert!(request(value.clone()).resolve().is_err(), "{value}");
        }
        assert!(
            serde_json::from_value::<ComputeRequest>(json!({"cpu":1,"memory":"1Gi","gpu":1}))
                .is_err()
        );
    }

    /// A worker declares its size as text and checks its cgroup against it, so the
    /// same rules a request is held to must apply: a worker configured with
    /// something no caller could request would be unreachable rather than refused.
    #[test]
    fn a_worker_declaration_is_held_to_the_request_rules() {
        assert_eq!(
            Resources::parse("2", "2Gi").unwrap(),
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB
            }
        );
        assert_eq!(
            Resources::parse(" 4 ", " 6Gi ").unwrap(),
            Resources {
                cpu: 4,
                memory_bytes: 6 * GIB
            }
        );
        for (cpu, memory) in [
            ("0", "1Gi"),
            ("1", "128Mi"),
            ("1", "1GB"),
            ("1", "1024"),
            ("two", "1Gi"),
            ("-1", "1Gi"),
            ("1.5", "1Gi"),
        ] {
            assert!(Resources::parse(cpu, memory).is_err(), "{cpu}/{memory}");
        }
    }

    #[test]
    fn the_engine_gets_half_the_container_cap() {
        assert_eq!(Resources::default().engine_memory(), "512MiB");
        assert_eq!(
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB
            }
            .engine_memory(),
            "1024MiB"
        );
        assert_eq!(
            Resources {
                cpu: 8,
                memory_bytes: 32 * GIB
            }
            .engine_memory(),
            "16384MiB"
        );
    }

    /// The mode survives being forwarded to the worker. Sending only the size would
    /// have an isolated session recorded as persistent, because a pod cannot tell
    /// from a request whether it is dedicated or shared with thirty others.
    #[test]
    fn a_placement_forwards_its_mode_as_well_as_its_size() {
        for mode in [Mode::Persistent, Mode::Isolated] {
            let placement = Placement {
                mode,
                resources: Resources {
                    cpu: 4,
                    memory_bytes: 6 * GIB,
                },
            };
            let forwarded = placement.request();
            assert_eq!(forwarded.compute, mode.name());
            let resolved = forwarded.resolve().unwrap();
            assert_eq!(resolved.mode, mode);
            assert_eq!(resolved.resources, placement.resources);
        }
    }

    #[test]
    fn a_resolved_size_round_trips_through_its_own_request() {
        for resources in [
            Resources::default(),
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB,
            },
            Resources {
                cpu: 4,
                memory_bytes: 1536 * MIB,
            },
        ] {
            assert_eq!(resources.request().resolve().unwrap().resources, resources);
        }
    }

    /// A request for isolation is well formed. Whether this deployment can place it
    /// is the router's question, exactly as it is for a size no worker provides.
    #[test]
    fn isolated_resolves_to_a_placement_the_router_then_judges() {
        let placement = request(json!({"compute":"isolated","cpu":4,"memory":"8Gi"}))
            .resolve()
            .unwrap();
        assert_eq!(placement.mode, Mode::Isolated);
        assert_eq!(
            placement.resources,
            Resources {
                cpu: 4,
                memory_bytes: 8 * GIB
            }
        );
    }

    /// Isolation is a guarantee, so where it cannot be provided the request is
    /// refused rather than served by shared compute. The code is distinguishable
    /// from a malformed request: one means ask differently, the other means ask
    /// elsewhere.
    #[test]
    fn unavailable_isolation_is_refused_and_never_downgraded() {
        let error = unavailable();
        assert_eq!(error.0, crate::failure::COMPUTE_UNAVAILABLE);
        assert!(error.0.retry_safe);
        assert!(error.1.contains("not downgraded"), "{}", error.1);
    }

    #[test]
    fn an_unknown_mode_stays_a_classified_request_error() {
        for value in [json!({"compute":"burst"}), json!({"compute":"Persistent"})] {
            let error = request(value.clone()).resolve().unwrap_err();
            assert_eq!(
                error.0.kind,
                crate::failure::INVALID_REQUEST.kind,
                "{value}"
            );
            assert!(error.1.contains("persistent or isolated"), "{}", error.1);
        }
    }

    #[test]
    fn placement_reports_the_mode_and_the_worker_that_served_it() {
        let described = request(json!({"cpu":2,"memory":"2Gi"}))
            .resolve()
            .unwrap()
            .describe("medium");
        assert_eq!(described["compute"], json!("persistent"));
        assert_eq!(described["worker_id"], json!("medium"));
        assert_eq!(described["cpu"], json!(2));
        assert_eq!(described["memory_bytes"], json!(2 * GIB));
    }

    /// Billing follows the request because a persistent worker is shared by up to
    /// 32 sessions, so charging for the provision would bill each one in full.
    #[test]
    fn billable_units_follow_the_requested_resources() {
        assert_eq!(resolved(json!({"cpu":1,"memory":"1Gi"})).units(), 1);
        assert_eq!(resolved(json!({"cpu":2,"memory":"2Gi"})).units(), 2);
        // Memory-heavy sizes bill on memory: 32 GiB / 4 outweighs 2 vCPU.
        assert_eq!(resolved(json!({"cpu":2,"memory":"32Gi"})).units(), 8);
    }
}
