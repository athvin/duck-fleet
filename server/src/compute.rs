//! Requested compute policy, resolved allocations, and worker-side validation.
//! Resource-only requests retain historical isolation; named persistent pools
//! select an allocation independently of its size. Observed placement is always
//! authored by the worker and never accepted as a client assertion.
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Authority {
    Reader,
    #[default]
    Writer,
}

impl Authority {
    pub fn name(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "reader" => Ok(Self::Reader),
            "writer" => Ok(Self::Writer),
            _ => Err("DUCKFLEET_AUTHORITY must be reader or writer".into()),
        }
    }
}

/// A compute size. Memory is the **container cap**, not the engine's share. Disk is
/// the pod's ephemeral-storage limit, which also bounds how far a query may spill.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
    pub fn usable(self) -> Result<(), String> {
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

/// Share of the container cap DuckDB is given on an isolated pod when nothing is
/// configured. The pod serves one session, so the engine may take the whole of
/// the half the process does not need for extensions, networking and Arrow results.
pub(crate) const ISOLATED_ENGINE_MEMORY_PERCENT: u64 = 50;
/// Share given on a persistent worker when its pool configures nothing. Lower than
/// the isolated share because a persistent worker holds many sessions at once: every
/// session's connection adds its own result buffers and Arrow batches outside the
/// engine's pool, so the margin outside the engine has to be wider.
pub(crate) const PERSISTENT_ENGINE_MEMORY_PERCENT: u64 = 40;
/// Bounds on a configured share. Above the ceiling the process has too little room
/// outside the engine and is OOM-killed instead of DuckDB reporting an error.
const ENGINE_MEMORY_PERCENT_RANGE: std::ops::RangeInclusive<u64> = 10..=90;
/// Smallest buffer pool the engine is started with, whatever the share works out to.
const MINIMUM_ENGINE_MEMORY: u64 = MINIMUM_MEMORY / 2;

/// Where the engine limits a worker started with came from. Recorded with its
/// placement, so history says whether a query ran under the automatic sizing or
/// under a hand-set one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EngineSource {
    /// Derived from the container size alone.
    Automatic,
    /// Set by a persistent pool's `engine` block.
    Configured,
    /// Set by `isolatedCompute.engineOverride`, which applies to every isolated pod.
    Override,
}
impl EngineSource {
    pub fn name(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::Configured => "configured",
            Self::Override => "override",
        }
    }
}

/// What the worker passes to DuckDB at startup. `memory_limit` and `threads` are
/// database-wide in DuckDB, not per connection, so on a persistent worker these are
/// one pool that every session on it shares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EngineLimits {
    pub memory_bytes: u64,
    pub threads: u16,
    pub source: EngineSource,
}

/// The engine settings a worker's environment asks for, still as text.
#[derive(Clone, Debug, Default)]
pub(crate) struct EngineSettings {
    /// `DUCKFLEET_ENGINE_MEMORY`: an absolute binary quantity. Persistent only, since
    /// an isolated pod's size changes with every request.
    pub memory: Option<String>,
    /// `DUCKFLEET_ENGINE_MEMORY_PERCENT`: a share of the container cap.
    pub memory_percent: Option<String>,
    /// `DUCKFLEET_ENGINE_THREADS`. On a persistent worker it must not exceed the
    /// CPU limit; on an isolated pod it is a ceiling, because one override has to fit
    /// every requested size.
    pub threads: Option<String>,
}
impl EngineSettings {
    pub fn from_env() -> Self {
        let read = |name| {
            std::env::var(name)
                .ok()
                .filter(|v: &String| !v.trim().is_empty())
        };
        Self {
            memory: read("DUCKFLEET_ENGINE_MEMORY"),
            memory_percent: read("DUCKFLEET_ENGINE_MEMORY_PERCENT"),
            threads: read("DUCKFLEET_ENGINE_THREADS"),
        }
    }
    fn is_empty(&self) -> bool {
        self.memory.is_none() && self.memory_percent.is_none() && self.threads.is_none()
    }

    /// Resolves the engine limits for a worker of this size and mode.
    ///
    /// Isolated pods size the engine from what the caller asked for unless the
    /// deployment sets an override. Persistent workers default lower and take their
    /// pool's settings. Either way a setting larger than the container is refused at
    /// startup rather than left for the kernel to settle with an OOM kill.
    pub fn resolve(&self, resources: Resources, mode: Mode) -> Result<EngineLimits, String> {
        let source = match (self.is_empty(), mode) {
            (true, _) => EngineSource::Automatic,
            (false, Mode::Persistent) => EngineSource::Configured,
            (false, Mode::Isolated) => EngineSource::Override,
        };
        let ceiling = resources.memory_bytes * ENGINE_MEMORY_PERCENT_RANGE.end() / 100;
        let memory_bytes = match (&self.memory, &self.memory_percent) {
            (Some(_), Some(_)) => {
                return Err(
                    "set DUCKFLEET_ENGINE_MEMORY or DUCKFLEET_ENGINE_MEMORY_PERCENT, not both"
                        .into(),
                );
            }
            (Some(_), None) if mode == Mode::Isolated => {
                return Err("an isolated pod's engine memory can only be overridden as a percentage (DUCKFLEET_ENGINE_MEMORY_PERCENT); its size changes per request".into());
            }
            (Some(memory), None) => {
                let bytes = parse_quantity(memory.trim()).ok_or_else(|| {
                    format!(
                        "DUCKFLEET_ENGINE_MEMORY {memory:?} must be a binary quantity such as 512Mi"
                    )
                })?;
                if bytes < MINIMUM_ENGINE_MEMORY || bytes > ceiling {
                    return Err(format!(
                        "DUCKFLEET_ENGINE_MEMORY {memory} must be between {}Mi and {}% of the {} container",
                        MINIMUM_ENGINE_MEMORY / MIB,
                        ENGINE_MEMORY_PERCENT_RANGE.end(),
                        resources.memory()
                    ));
                }
                bytes
            }
            (None, percent) => {
                let percent = match percent {
                    None if mode == Mode::Isolated => ISOLATED_ENGINE_MEMORY_PERCENT,
                    None => PERSISTENT_ENGINE_MEMORY_PERCENT,
                    Some(text) => text
                        .trim()
                        .parse::<u64>()
                        .ok()
                        .filter(|p| ENGINE_MEMORY_PERCENT_RANGE.contains(p))
                        .ok_or_else(|| {
                            format!(
                                "DUCKFLEET_ENGINE_MEMORY_PERCENT {text:?} must be a whole number from {} to {}",
                                ENGINE_MEMORY_PERCENT_RANGE.start(),
                                ENGINE_MEMORY_PERCENT_RANGE.end()
                            )
                        })?,
                };
                (resources.memory_bytes * percent / 100).max(MINIMUM_ENGINE_MEMORY)
            }
        };
        let threads = match &self.threads {
            None => resources.cpu,
            Some(text) => {
                let threads = text
                    .trim()
                    .parse::<u16>()
                    .ok()
                    .filter(|t| *t >= 1)
                    .ok_or_else(|| {
                        format!(
                            "DUCKFLEET_ENGINE_THREADS {text:?} must be a whole number of at least 1"
                        )
                    })?;
                match mode {
                    Mode::Isolated => threads.min(resources.cpu),
                    Mode::Persistent if threads > resources.cpu => {
                        return Err(format!(
                            "DUCKFLEET_ENGINE_THREADS {threads} exceeds the worker's {} CPU",
                            resources.cpu
                        ));
                    }
                    Mode::Persistent => threads,
                }
            }
        };
        Ok(EngineLimits {
            memory_bytes,
            threads,
            source,
        })
    }
}
impl EngineLimits {
    /// The `memory_limit` value DuckDB is given, rounded down to whole MiB.
    pub fn memory(self) -> String {
        format!("{}MiB", self.memory_bytes / MIB)
    }
}

/// Renders a byte count as the binary quantity Kubernetes and Docker use.
fn quantity(bytes: u64) -> String {
    if bytes.is_multiple_of(GIB) {
        format!("{}Gi", bytes / GIB)
    } else if bytes.is_multiple_of(MIB) {
        format!("{}Mi", bytes / MIB)
    } else {
        // Accepted quantities are integral KiB. Preserve that precision when
        // forwarding allocations into pod resources and worker declarations.
        format!("{}Ki", bytes / 1024)
    }
}

/// Parses a binary quantity such as `2Gi` or `512Mi`.
///
/// A suffix is required: a bare number would be ambiguous between bytes and
/// mebibytes, and both Kubernetes and Docker express a container cap with a suffix.
pub(crate) fn parse_quantity(text: &str) -> Option<u64> {
    for (suffix, multiplier) in [("Gi", GIB), ("Mi", MIB), ("Ki", 1024)] {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.trim().parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    None
}

/// Original caller intent. Serialization deliberately omits absent fields;
/// durable fingerprint versions implement their own historical projections.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputeRequest {
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Requirements {
    pub cpu: Option<u16>,
    pub memory_bytes: Option<u64>,
    pub disk_bytes: Option<u64>,
}
impl Requirements {
    pub fn satisfied_by(self, resources: Resources) -> bool {
        self.cpu.is_none_or(|minimum| resources.cpu >= minimum)
            && self
                .memory_bytes
                .is_none_or(|minimum| resources.memory_bytes >= minimum)
            && self
                .disk_bytes
                .is_none_or(|minimum| resources.disk_bytes >= minimum)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Selection {
    Persistent {
        pool: Option<String>,
        minimum: Requirements,
    },
    Isolated(Resources),
}

/// A resolved policy. Resources are the allocation actually selected and billed,
/// not the optional minimum requirements in the original request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Placement {
    pub mode: Mode,
    pub pool: Option<String>,
    pub resources: Resources,
    #[serde(default)]
    pub authority: Authority,
}

pub(crate) fn valid_pool_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 40
        && bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

impl ComputeRequest {
    /// Syntax-only validation: this does not require a configured pool registry.
    pub fn validate(&self) -> Result<(), ApiError> {
        self.selection().map(|_| ())
    }

    /// Fixed semantic policy input for fingerprint v2. Original textual intent
    /// remains in diagnostics, while equivalent binary quantities hash alike.
    pub fn canonical_intent(&self) -> Result<Value, ApiError> {
        let minimum = match self.selection()? {
            Selection::Persistent { minimum, .. } => minimum,
            Selection::Isolated(resources) => Requirements {
                cpu: self.cpu,
                memory_bytes: self.memory.as_ref().map(|_| resources.memory_bytes),
                disk_bytes: self.disk.as_ref().map(|_| resources.disk_bytes),
            },
        };
        Ok(json!({"mode":self.mode.map(Mode::name),"pool":self.pool,
            "cpu":minimum.cpu,"memory_bytes":minimum.memory_bytes,"disk_bytes":minimum.disk_bytes}))
    }

    pub fn selection(&self) -> Result<Selection, ApiError> {
        if self
            .pool
            .as_deref()
            .is_some_and(|pool| !valid_pool_name(pool))
        {
            return Err(ApiError::bad(
                "pool must be a lowercase DNS label starting with a letter, at most 40 characters",
            ));
        }
        if self.mode == Some(Mode::Isolated) && self.pool.is_some() {
            return Err(ApiError::bad(
                "isolated compute cannot select a persistent pool",
            ));
        }
        let has_resources = self.cpu.is_some() || self.memory.is_some() || self.disk.is_some();
        let mode = self
            .mode
            .unwrap_or(if self.pool.is_some() || !has_resources {
                Mode::Persistent
            } else {
                Mode::Isolated
            });
        if mode == Mode::Isolated {
            let base = Resources::default();
            return Ok(Selection::Isolated(
                Resources::parse(
                    &self.cpu.unwrap_or(base.cpu).to_string(),
                    self.memory.as_deref().unwrap_or("1Gi"),
                    self.disk.as_deref().unwrap_or("2Gi"),
                )
                .map_err(ApiError::bad)?,
            ));
        }
        if self.cpu == Some(0) {
            return Err(ApiError::bad("cpu must be at least 1"));
        }
        let bytes = |value: &Option<String>, dimension: &str| -> Result<Option<u64>, ApiError> {
            value
                .as_deref()
                .map(|text| {
                    parse_quantity(text.trim())
                        .filter(|bytes| *bytes > 0)
                        .ok_or_else(|| {
                            ApiError::bad(format!(
                                "{dimension} must be a positive binary quantity such as 2Gi"
                            ))
                        })
                })
                .transpose()
        };
        Ok(Selection::Persistent {
            pool: self.pool.clone(),
            minimum: Requirements {
                cpu: self.cpu,
                memory_bytes: bytes(&self.memory, "memory")?,
                disk_bytes: bytes(&self.disk, "disk")?,
            },
        })
    }

    pub fn validate_resolved(&self, resolved: &Placement) -> Result<(), ApiError> {
        resolved.resources.usable().map_err(ApiError::bad)?;
        let matches = match self.selection()? {
            Selection::Persistent { pool, minimum } => {
                resolved.mode == Mode::Persistent
                    && resolved.pool.as_deref().is_some_and(valid_pool_name)
                    && pool
                        .as_ref()
                        .is_none_or(|name| resolved.pool.as_ref() == Some(name))
                    && minimum.satisfied_by(resolved.resources)
            }
            Selection::Isolated(resources) => {
                resolved.mode == Mode::Isolated
                    && resolved.pool.is_none()
                    && resolved.resources == resources
            }
        };
        if !matches {
            return Err(ApiError::bad(
                "resolved compute does not satisfy the original request",
            ));
        }
        Ok(())
    }

    /// Direct worker callers can choose only this worker's pool/allocation.
    /// A dedicated worker is represented by pool=None and cannot serve persistent work.
    pub fn resolve_local(
        &self,
        pool: Option<&str>,
        resources: Resources,
        authority: Authority,
    ) -> Result<Placement, ApiError> {
        let resolved = match self.selection()? {
            Selection::Persistent { .. } => Placement {
                mode: Mode::Persistent,
                pool: Some(
                    pool.ok_or_else(|| {
                        ApiError::bad("this dedicated worker cannot serve persistent sessions")
                    })?
                    .to_owned(),
                ),
                resources,
                authority,
            },
            Selection::Isolated(resources) => Placement {
                mode: Mode::Isolated,
                pool: None,
                resources,
                authority,
            },
        };
        self.validate_resolved(&resolved)?;
        resolved.validate_worker(pool, resources, authority)?;
        Ok(resolved)
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
    pub fn validate_worker(
        &self,
        pool: Option<&str>,
        resources: Resources,
        authority: Authority,
    ) -> Result<(), ApiError> {
        if self.mode == Mode::Isolated && pool.is_some() {
            return Err(unavailable());
        }
        let mode = if pool.is_some() {
            Mode::Persistent
        } else {
            Mode::Isolated
        };
        if self.mode != mode
            || self.pool.as_deref() != pool
            || self.resources != resources
            || self.authority != authority
        {
            return Err(ApiError::bad(
                "resolved compute does not match this worker's pool, resources, and authority",
            ));
        }
        self.resources.usable().map_err(ApiError::bad)
    }

    /// Existing flattened observed-placement fields stay compatible with clients.
    pub fn describe(&self, worker_id: &str) -> Value {
        let mut value = self.resources.describe();
        value["worker_id"] = json!(worker_id);
        value["compute"] = json!(self.mode.name());
        value["pool"] = json!(self.pool);
        value["authority"] = json!(self.authority.name());
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
    pub fn validate(&self) -> Result<(), ApiError> {
        if !self.context.is_object() || self.context.to_string().len() > 8192 {
            return Err(ApiError::bad(
                "session context must be an object of at most 8192 bytes",
            ));
        }
        self.compute.validate()
    }
}

/// Accepted only on the authenticated internal worker hop. Public gateways parse
/// SessionRequest instead and reject this envelope's authoritative fields.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForwardedSessionRequest {
    pub version: u8,
    pub request: SessionRequest,
    pub resolved: Placement,
    pub registry_revision: String,
}
impl ForwardedSessionRequest {
    pub fn validate_version(&self) -> Result<(), ApiError> {
        if self.version != 1 {
            return Err(ApiError::bad("unsupported worker forwarding version"));
        }
        self.request.validate()?;
        self.request.compute.validate_resolved(&self.resolved)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum WorkerSessionRequest {
    Direct(SessionRequest),
    Forwarded(ForwardedSessionRequest),
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(value: Value) -> ComputeRequest {
        serde_json::from_value(value).expect("should deserialize")
    }
    fn placement(value: Value) -> Placement {
        let request = request(value);
        match request.selection().unwrap() {
            Selection::Persistent { .. } => request
                .resolve_local(Some("small"), Resources::default(), Authority::Writer)
                .unwrap(),
            Selection::Isolated(resources) => request
                .resolve_local(None, resources, Authority::Writer)
                .unwrap(),
        }
    }

    /// The whole contract in one test: nothing named is the shared default, anything
    /// named is a pod of its own.
    #[test]
    fn no_resources_is_shared_and_any_resource_is_isolated() {
        let shared = placement(json!({}));
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
            assert!(request(value.clone()).validate().is_err(), "{value}");
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
    fn resource_rendering_preserves_accepted_kibibytes_without_downgrading() {
        let resources = Resources::parse("2", "262148Ki", "2097156Ki").unwrap();
        assert_eq!(resources.memory(), "262148Ki");
        assert_eq!(resources.disk(), "2097156Ki");
        assert_eq!(
            Resources::parse("2", &resources.memory(), &resources.disk()).unwrap(),
            resources
        );
    }

    fn settings(
        memory: Option<&str>,
        percent: Option<&str>,
        threads: Option<&str>,
    ) -> EngineSettings {
        EngineSettings {
            memory: memory.map(str::to_owned),
            memory_percent: percent.map(str::to_owned),
            threads: threads.map(str::to_owned),
        }
    }

    /// With nothing configured an isolated pod gets half its cap and a thread per CPU:
    /// the engine is sized from what the caller asked for.
    #[test]
    fn an_isolated_engine_is_sized_from_the_request() {
        let limits = EngineSettings::default()
            .resolve(
                Resources::parse("4", "8Gi", "10Gi").unwrap(),
                Mode::Isolated,
            )
            .unwrap();
        assert_eq!(limits.memory(), "4096MiB");
        assert_eq!(limits.threads, 4);
        assert_eq!(limits.source, EngineSource::Automatic);
        assert_eq!(
            EngineSettings::default()
                .resolve(Resources::default(), Mode::Isolated)
                .unwrap()
                .memory(),
            "512MiB"
        );
    }

    /// A shared worker defaults lower than an isolated pod of the same size.
    #[test]
    fn a_persistent_engine_defaults_below_the_isolated_share() {
        let resources = Resources::parse("2", "4Gi", "4Gi").unwrap();
        let limits = EngineSettings::default()
            .resolve(resources, Mode::Persistent)
            .unwrap();
        assert_eq!(limits.memory_bytes, 4 * GIB * 40 / 100);
        assert_eq!(limits.threads, 2);
        assert_eq!(limits.source, EngineSource::Automatic);
        // The smallest container still gets a workable buffer pool.
        let smallest = Resources::parse("1", "256Mi", "1Gi").unwrap();
        assert_eq!(
            EngineSettings::default()
                .resolve(smallest, Mode::Persistent)
                .unwrap()
                .memory(),
            "128MiB"
        );
    }

    #[test]
    fn a_pool_may_set_its_engine_but_not_beyond_its_container() {
        let resources = Resources::parse("4", "8Gi", "4Gi").unwrap();
        let limits = settings(Some("2Gi"), None, Some("2"))
            .resolve(resources, Mode::Persistent)
            .unwrap();
        assert_eq!((limits.memory(), limits.threads), ("2048MiB".into(), 2));
        assert_eq!(limits.source, EngineSource::Configured);
        let limits = settings(None, Some("25"), None)
            .resolve(resources, Mode::Persistent)
            .unwrap();
        assert_eq!(limits.memory_bytes, 8 * GIB * 25 / 100);
        for refused in [
            settings(Some("8Gi"), None, None),
            settings(Some("64Mi"), None, None),
            settings(Some("2GB"), None, None),
            settings(Some("2Gi"), Some("25"), None),
            settings(None, Some("95"), None),
            settings(None, Some("5"), None),
            settings(None, None, Some("5")),
            settings(None, None, Some("0")),
        ] {
            assert!(
                refused.resolve(resources, Mode::Persistent).is_err(),
                "{refused:?}"
            );
        }
    }

    /// The isolated override is deliberate and visible: it is a share rather than a
    /// size, its thread count is a ceiling so one value fits every request, and the
    /// worker records it as an override.
    #[test]
    fn an_isolated_override_scales_with_the_request() {
        let small = Resources::parse("1", "2Gi", "2Gi").unwrap();
        let large = Resources::parse("8", "32Gi", "20Gi").unwrap();
        let override_ = settings(None, Some("70"), Some("4"));
        let limits = override_.resolve(small, Mode::Isolated).unwrap();
        assert_eq!(
            (limits.memory_bytes, limits.threads),
            (2 * GIB * 70 / 100, 1)
        );
        assert_eq!(limits.source, EngineSource::Override);
        let limits = override_.resolve(large, Mode::Isolated).unwrap();
        assert_eq!(
            (limits.memory_bytes, limits.threads),
            (32 * GIB * 70 / 100, 4)
        );
        assert!(
            settings(Some("1Gi"), None, None)
                .resolve(large, Mode::Isolated)
                .is_err()
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

    #[test]
    fn forwarding_preserves_original_requirements_and_rejects_spoofed_resolution() {
        let requested = request(json!({"pool":"interactive", "cpu":2}));
        let resources = Resources::parse("4", "16Gi", "20Gi").unwrap();
        let resolved = requested
            .resolve_local(Some("interactive"), resources, Authority::Writer)
            .unwrap();
        assert_eq!(
            serde_json::to_value(&requested).unwrap(),
            json!({"pool":"interactive", "cpu":2})
        );
        let forwarded = ForwardedSessionRequest {
            version: 1,
            request: SessionRequest {
                compute: requested.clone(),
                context: json!({}),
            },
            resolved: resolved.clone(),
            registry_revision: "test".into(),
        };
        assert!(forwarded.validate_version().is_ok());
        assert!(serde_json::from_value::<SessionRequest>(json!(forwarded)).is_err());
        let mut wrong = resolved.clone();
        wrong.pool = Some("another".into());
        assert!(requested.validate_resolved(&wrong).is_err());
        wrong = resolved;
        wrong.resources.disk_bytes /= 2;
        assert!(
            wrong
                .validate_worker(Some("interactive"), resources, Authority::Writer)
                .is_err()
        );
    }

    #[test]
    fn persistent_minima_are_independent_and_isolated_defaults_remain_historical() {
        let resources = Resources::parse("2", "512Mi", "1Gi").unwrap();
        assert!(
            request(json!({"mode":"persistent","cpu":2}))
                .resolve_local(Some("tiny"), resources, Authority::Writer)
                .is_ok()
        );
        assert!(
            request(json!({"pool":"tiny","memory":"128Mi"}))
                .resolve_local(Some("tiny"), resources, Authority::Writer)
                .is_ok()
        );
        assert!(
            request(json!({"pool":"tiny","disk":"2Gi"}))
                .resolve_local(Some("tiny"), resources, Authority::Writer)
                .is_err()
        );
        assert!(
            request(json!({"mode":"isolated","pool":"tiny"}))
                .validate()
                .is_err()
        );
        assert!(
            request(json!({"pool":"other"}))
                .resolve_local(Some("tiny"), resources, Authority::Writer)
                .is_err()
        );
        assert!(
            request(json!({"cpu":2}))
                .resolve_local(Some("tiny"), resources, Authority::Writer)
                .is_err()
        );
        assert_eq!(
            placement(json!({"mode":"isolated"})).resources,
            Resources::default()
        );
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
        let default = placement(json!({})).describe("small");
        assert_eq!(default["compute"], json!("persistent"));
    }

    /// Billing follows the request because a persistent worker is shared by many
    /// sessions, and disk never enters the price.
    #[test]
    fn billable_units_follow_cpu_and_memory_not_disk() {
        assert_eq!(placement(json!({})).resources.units(), 1);
        assert_eq!(
            placement(json!({"cpu": 2, "memory": "2Gi"}))
                .resources
                .units(),
            2
        );
        // Memory-heavy sizes bill on memory: 32 GiB / 4 outweighs 2 vCPU.
        assert_eq!(
            placement(json!({"cpu": 2, "memory": "32Gi"}))
                .resources
                .units(),
            8
        );
        // A huge disk on an otherwise default size is still one unit.
        assert_eq!(placement(json!({"disk": "1000Gi"})).resources.units(), 1);
    }
}
