//! What a caller may ask for, and what that resolves to.
//!
//! The contract is deliberately small: a request that names no resources runs on the
//! shared persistent tier at the default size, and a request that names *any* of cpu,
//! memory, or disk gets a pod of its own, sized to the ask. There is no mode to pick —
//! asking for something other than the default *is* the request for isolation. A dbt
//! model that sets nothing under `meta.platform` shares the prestarted workers; one
//! that sets a resource there triggers a dedicated pod.
//!
//! Resources are carried as values rather than named classes. Adding a size is running
//! a pod with those limits rather than teaching the platform a new name.
//!
//! A worker still declares what it is meant to be and checks its cgroup against that
//! declaration, so a container that did not get the limits it was created with refuses
//! to start instead of quietly serving a different size.
use crate::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
/// Below this the engine's half share is too small to load extensions and hold a
/// result, so it is refused rather than started and left to fail obscurely.
const MINIMUM_MEMORY: u64 = 256 * MIB;
/// Below this an isolated pod has no room for the image's writable needs plus any
/// spill, so a size this small is refused rather than evicted the moment it spills.
const MINIMUM_DISK: u64 = GIB;
/// Headroom left between the disk a caller asked for and the spill budget the engine
/// is given, so DuckDB reports an out-of-space error rather than the kubelet evicting
/// the pod when its ephemeral storage fills.
const DISK_HEADROOM: u64 = 512 * MIB;

/// Where a statement runs. Derived from the request rather than chosen by the caller:
/// a request that names resources is a request for a pod of its own.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A prestarted worker, shared with other sessions. What a request with no
    /// resource ask resolves to.
    #[default]
    Persistent,
    /// A worker created for this session alone and discarded afterwards. What any
    /// resource ask resolves to. Served only where the gateway holds the pod grant
    /// (see `isolated.rs`); refused with `COMPUTE_UNAVAILABLE` everywhere else, and
    /// never downgraded to a shared worker.
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

/// A compute size. Memory is the **container cap**, not the engine's share. Disk is
/// the pod's ephemeral-storage limit, which also bounds how far a query may spill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Resources {
    pub cpu: u16,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            cpu: 1,
            memory_bytes: GIB,
            disk_bytes: 2 * GIB,
        }
    }
}
impl Resources {
    /// Parses a size from text, applying the same rules a request is held to.
    ///
    /// Shared with the worker's own configuration so the two cannot disagree about
    /// what a valid size is: a worker configured with something a caller could never
    /// request would be unreachable, which is worse than refusing to start.
    pub fn parse(cpu: &str, memory: &str, disk: &str) -> Result<Self, String> {
        let cpu: u16 = cpu
            .trim()
            .parse()
            .map_err(|_| format!("cpu {cpu:?} must be a whole number"))?;
        let memory_bytes = parse_quantity(memory.trim())
            .ok_or_else(|| format!("memory {memory:?} must be a binary quantity such as 2Gi"))?;
        let disk_bytes = parse_quantity(disk.trim())
            .ok_or_else(|| format!("disk {disk:?} must be a binary quantity such as 10Gi"))?;
        let resources = Self {
            cpu,
            memory_bytes,
            disk_bytes,
        };
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
        if self.disk_bytes < MINIMUM_DISK {
            return Err(format!(
                "disk must be at least {}Gi; a smaller pod has no room to spill",
                MINIMUM_DISK / GIB
            ));
        }
        Ok(())
    }
    /// DuckDB is held to half the container cap, leaving room for extensions,
    /// networking and Arrow results. This is the rule the README documents.
    pub fn engine_memory(self) -> String {
        format!("{}MiB", self.memory_bytes / 2 / MIB)
    }
    /// How far the engine may spill to local disk, kept below the ephemeral-storage
    /// limit by a fixed margin so DuckDB errors before the kubelet evicts the pod.
    /// Asking for more disk is therefore how a caller makes room for a larger
    /// out-of-core sort or join.
    pub fn spill_limit(self) -> String {
        let spill = self.disk_bytes.saturating_sub(DISK_HEADROOM).max(256 * MIB);
        format!("{}MiB", spill / MIB)
    }
    /// Normalized billable size, from the resources rather than from a class name.
    /// Disk does not enter the price: it is a bound on a pod, not a metered dimension.
    pub fn units(self) -> i64 {
        crate::pricing::normalized_units(self.cpu, self.memory_bytes)
    }
    pub fn memory(self) -> String {
        quantity(self.memory_bytes)
    }
    pub fn disk(self) -> String {
        quantity(self.disk_bytes)
    }
    pub fn describe(self) -> Value {
        json!({"cpu": self.cpu, "memory_bytes": self.memory_bytes, "disk_bytes": self.disk_bytes})
    }
}

/// Renders a byte count as the binary quantity Kubernetes and Docker use.
fn quantity(bytes: u64) -> String {
    if bytes.is_multiple_of(GIB) {
        format!("{}Gi", bytes / GIB)
    } else {
        format!("{}Mi", bytes / MIB)
    }
}

/// Parses a binary quantity such as `2Gi` or `512Mi`.
///
/// A suffix is required: a bare number would be ambiguous between bytes and
/// mebibytes, and both Kubernetes and Docker express a container cap with a suffix.
fn parse_quantity(text: &str) -> Option<u64> {
    for (suffix, multiplier) in [("Gi", GIB), ("Mi", MIB), ("Ki", 1024)] {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.trim().parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    None
}

/// What a caller asks for. Every field is optional, and which ones are present is the
/// whole contract: none means the shared default, any means a pod sized to the ask
/// with the rest filled from the default.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputeRequest {
    pub cpu: Option<u16>,
    pub memory: Option<String>,
    pub disk: Option<String>,
}

/// Where a resolved request will run. Both halves are reported to the caller and
/// recorded in history, so a placement can be compared against what was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Placement {
    pub mode: Mode,
    pub resources: Resources,
}

impl ComputeRequest {
    /// Resolves a request to a placement, or refuses it.
    ///
    /// No resource named is the shared default. Any named is a pod of its own, with
    /// the unspecified dimensions filled from the default so a caller can tune a
    /// single axis — `cpu` alone keeps the default memory and disk.
    ///
    /// Only the request itself is judged here. Whether this deployment can create a
    /// pod at all is the router's question, and answering it here would turn a
    /// deployment without isolated compute into a malformed request.
    pub fn resolve(&self) -> Result<Placement, ApiError> {
        if self.cpu.is_none() && self.memory.is_none() && self.disk.is_none() {
            return Ok(Placement {
                mode: Mode::Persistent,
                resources: Resources::default(),
            });
        }
        let base = Resources::default();
        let resources = Resources::parse(
            &self.cpu.unwrap_or(base.cpu).to_string(),
            &self.memory.clone().unwrap_or_else(|| base.memory()),
            &self.disk.clone().unwrap_or_else(|| base.disk()),
        )
        .map_err(ApiError::bad)?;
        Ok(Placement {
            mode: Mode::Isolated,
            resources,
        })
    }
}

/// Refused because this deployment cannot run isolated compute, which most cannot:
/// the gateway needs RBAC to create pods and the deploy asserts it cannot grant
/// itself any. Defined here, beside the contract, so the one sentence that promises
/// no downgrade cannot drift from the mode it describes.
pub(crate) fn unavailable() -> ApiError {
    ApiError::new(
        crate::failure::COMPUTE_UNAVAILABLE,
        "this request asked for a dedicated size, which runs on a pod of its own, and \
         this deployment cannot create pods; no statement ran and the request was not \
         downgraded to a shared worker",
    )
}

impl Placement {
    /// Billable size comes from what the caller requested, not from the worker it
    /// landed on. A persistent worker is shared by many sessions, so charging for the
    /// provision would bill each one for the whole worker.
    pub fn units(self) -> i64 {
        self.resources.units()
    }
    /// The request the gateway forwards to the worker.
    ///
    /// A persistent placement forwards nothing, so the worker re-derives the shared
    /// default exactly as a direct caller would; an isolated placement forwards the
    /// fully resolved size, so the pod re-derives isolation from the same presence
    /// rule and records the mode it actually served. The derivation is identical on
    /// both hops, which is what keeps the gateway and the worker from disagreeing
    /// about what a request meant.
    pub fn request(self) -> ComputeRequest {
        match self.mode {
            Mode::Persistent => ComputeRequest::default(),
            Mode::Isolated => ComputeRequest {
                cpu: Some(self.resources.cpu),
                memory: Some(self.resources.memory()),
                disk: Some(self.resources.disk()),
            },
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
            compute: ComputeRequest::default(),
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
    fn placement(value: Value) -> Placement {
        request(value).resolve().unwrap()
    }

    /// The whole contract in one test: nothing named is the shared default, anything
    /// named is a pod of its own.
    #[test]
    fn no_resources_is_shared_and_any_resource_is_isolated() {
        let shared = SessionRequest::default().validate().unwrap();
        assert_eq!(shared.mode, Mode::Persistent);
        assert_eq!(shared.resources, Resources::default());
        // An empty compute object is still the default.
        assert_eq!(placement(json!({})).mode, Mode::Persistent);
        // Any single dimension flips it to a dedicated pod.
        for named in [
            json!({"cpu": 2}),
            json!({"memory": "4Gi"}),
            json!({"disk": "20Gi"}),
        ] {
            assert_eq!(placement(named.clone()).mode, Mode::Isolated, "{named}");
        }
    }

    /// A partial ask keeps the default for the dimensions it does not name, so a
    /// caller can tune one axis without restating the others.
    #[test]
    fn unspecified_dimensions_fall_back_to_the_default() {
        let base = Resources::default();
        let cpu_only = placement(json!({"cpu": 8})).resources;
        assert_eq!(cpu_only.cpu, 8);
        assert_eq!(cpu_only.memory_bytes, base.memory_bytes);
        assert_eq!(cpu_only.disk_bytes, base.disk_bytes);
        let disk_only = placement(json!({"disk": "50Gi"})).resources;
        assert_eq!(disk_only.cpu, base.cpu);
        assert_eq!(disk_only.memory_bytes, base.memory_bytes);
        assert_eq!(disk_only.disk_bytes, 50 * GIB);
        // A fully named ask resolves to exactly itself.
        assert_eq!(
            placement(json!({"cpu": 4, "memory": "16Gi", "disk": "100Gi"})).resources,
            Resources {
                cpu: 4,
                memory_bytes: 16 * GIB,
                disk_bytes: 100 * GIB,
            }
        );
    }

    /// Any size resolves, including ones nothing was prestarted for. Whether this
    /// deployment can place it is the router's question, not this one's.
    #[test]
    fn a_size_outside_any_prestarted_worker_resolves() {
        assert_eq!(
            placement(json!({"cpu": 16, "memory": "128Gi", "disk": "500Gi"})).resources,
            Resources {
                cpu: 16,
                memory_bytes: 128 * GIB,
                disk_bytes: 500 * GIB,
            }
        );
        assert_eq!(
            placement(json!({"cpu": 1, "memory": "512Mi"}))
                .resources
                .memory_bytes,
            512 * MIB
        );
    }

    #[test]
    fn unusable_requests_are_refused() {
        for value in [
            json!({"cpu": 0, "memory": "1Gi"}),
            json!({"memory": "128Mi"}),
            json!({"disk": "256Mi"}),
            json!({"memory": "1GB"}),
            json!({"memory": "1024"}),
            json!({"disk": "lots"}),
        ] {
            assert!(request(value.clone()).resolve().is_err(), "{value}");
        }
        // The old mode string and any unknown dimension are rejected at the wire.
        assert!(serde_json::from_value::<ComputeRequest>(json!({"compute": "isolated"})).is_err());
        assert!(serde_json::from_value::<ComputeRequest>(json!({"gpu": 1})).is_err());
    }

    /// A worker declares its size as text and checks its cgroup against it, so the
    /// same rules a request is held to must apply.
    #[test]
    fn a_worker_declaration_is_held_to_the_request_rules() {
        assert_eq!(
            Resources::parse("2", "2Gi", "10Gi").unwrap(),
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB,
                disk_bytes: 10 * GIB,
            }
        );
        assert_eq!(
            Resources::parse(" 4 ", " 6Gi ", " 20Gi ").unwrap(),
            Resources {
                cpu: 4,
                memory_bytes: 6 * GIB,
                disk_bytes: 20 * GIB,
            }
        );
        for (cpu, memory, disk) in [
            ("0", "1Gi", "2Gi"),
            ("1", "128Mi", "2Gi"),
            ("1", "1Gi", "256Mi"),
            ("1", "1GB", "2Gi"),
            ("two", "1Gi", "2Gi"),
            ("1.5", "1Gi", "2Gi"),
        ] {
            assert!(
                Resources::parse(cpu, memory, disk).is_err(),
                "{cpu}/{memory}/{disk}"
            );
        }
    }

    #[test]
    fn the_engine_gets_half_the_container_cap() {
        assert_eq!(Resources::default().engine_memory(), "512MiB");
        assert_eq!(
            Resources {
                cpu: 2,
                memory_bytes: 2 * GIB,
                disk_bytes: 10 * GIB,
            }
            .engine_memory(),
            "1024MiB"
        );
    }

    /// Spill tracks disk, held below the ephemeral-storage limit by a fixed margin.
    #[test]
    fn the_spill_budget_follows_the_disk_but_stays_under_it() {
        assert_eq!(Resources::default().spill_limit(), "1536MiB");
        assert_eq!(
            Resources {
                cpu: 1,
                memory_bytes: GIB,
                disk_bytes: 100 * GIB,
            }
            .spill_limit(),
            format!("{}MiB", (100 * GIB - DISK_HEADROOM) / MIB)
        );
        // The smallest allowed disk still leaves a usable floor rather than zero.
        assert_eq!(
            Resources {
                cpu: 1,
                memory_bytes: GIB,
                disk_bytes: MINIMUM_DISK,
            }
            .spill_limit(),
            "512MiB"
        );
    }

    /// A persistent placement forwards nothing so the worker re-derives the default;
    /// an isolated one forwards its full size so the pod re-derives isolation.
    #[test]
    fn a_placement_round_trips_through_the_request_it_forwards() {
        let persistent = Placement {
            mode: Mode::Persistent,
            resources: Resources::default(),
        };
        let forwarded = persistent.request();
        assert_eq!(forwarded.cpu, None);
        assert_eq!(forwarded.memory, None);
        assert_eq!(forwarded.disk, None);
        assert_eq!(forwarded.resolve().unwrap(), persistent);

        let isolated = Placement {
            mode: Mode::Isolated,
            resources: Resources {
                cpu: 4,
                memory_bytes: 6 * GIB,
                disk_bytes: 40 * GIB,
            },
        };
        let resolved = isolated.request().resolve().unwrap();
        assert_eq!(resolved, isolated);
    }

    /// Isolation is a guarantee, so where it cannot be provided the request is
    /// refused rather than served by shared compute.
    #[test]
    fn unavailable_isolation_is_refused_and_never_downgraded() {
        let error = unavailable();
        assert_eq!(error.0, crate::failure::COMPUTE_UNAVAILABLE);
        assert!(error.0.retry_safe);
        assert!(error.1.contains("not"), "{}", error.1);
        assert!(error.1.contains("downgraded"), "{}", error.1);
    }

    #[test]
    fn placement_reports_the_mode_the_size_and_the_worker() {
        let described =
            placement(json!({"cpu": 2, "memory": "2Gi", "disk": "10Gi"})).describe("session-abc");
        assert_eq!(described["compute"], json!("isolated"));
        assert_eq!(described["worker_id"], json!("session-abc"));
        assert_eq!(described["cpu"], json!(2));
        assert_eq!(described["memory_bytes"], json!(2 * GIB));
        assert_eq!(described["disk_bytes"], json!(10 * GIB));
        // The shared default reports persistent and the default size.
        let default = SessionRequest::default()
            .validate()
            .unwrap()
            .describe("small");
        assert_eq!(default["compute"], json!("persistent"));
    }

    /// Billing follows the request because a persistent worker is shared by many
    /// sessions, and disk never enters the price.
    #[test]
    fn billable_units_follow_cpu_and_memory_not_disk() {
        assert_eq!(placement(json!({})).units(), 1);
        assert_eq!(placement(json!({"cpu": 2, "memory": "2Gi"})).units(), 2);
        // Memory-heavy sizes bill on memory: 32 GiB / 4 outweighs 2 vCPU.
        assert_eq!(placement(json!({"cpu": 2, "memory": "32Gi"})).units(), 8);
        // A huge disk on an otherwise default size is still one unit.
        assert_eq!(placement(json!({"disk": "1000Gi"})).units(), 1);
    }
}
