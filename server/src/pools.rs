//! Immutable, operator-managed persistent compute pools. Reloading swaps a whole
//! validated registry; sessions retain their resolved placement independently.
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write,
    path::PathBuf,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    ApiError,
    compute::{ComputeRequest, Mode, Placement, Resources, Selection, valid_pool_name},
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Pool {
    pub resources: Resources,
    pub endpoints: Vec<String>,
    pub max_sessions_per_worker: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PoolRegistry {
    pub default_pool: String,
    pub pools: BTreeMap<String, Pool>,
    pub revision: String,
    pub legacy: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    default_pool: String,
    pools: BTreeMap<String, PoolConfiguration>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolConfiguration {
    cpu: u16,
    memory: String,
    disk: String,
    endpoints: Vec<String>,
    #[serde(default = "default_sessions")]
    max_sessions_per_worker: usize,
}
fn default_sessions() -> usize {
    32
}

fn endpoint(value: &str) -> Result<String> {
    let url = reqwest::Url::parse(value.trim()).context("invalid pool worker endpoint")?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "pool worker endpoint must be an HTTP(S) origin without credentials, path, query, or fragment"
    );
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

impl PoolRegistry {
    pub fn load() -> Result<(Self, Option<PathBuf>)> {
        let file = std::env::var("DUCKFLEET_POOLS_FILE").ok();
        let inline = std::env::var("DUCKFLEET_POOLS_JSON").ok();
        anyhow::ensure!(
            file.is_none() || inline.is_none(),
            "configure only one of DUCKFLEET_POOLS_FILE and DUCKFLEET_POOLS_JSON"
        );
        if file.is_some() || inline.is_some() {
            anyhow::ensure!(
                std::env::var("DUCKFLEET_WORKER_URLS")
                    .ok()
                    .is_none_or(|urls| urls.trim().is_empty()),
                "remove legacy DUCKFLEET_WORKER_URLS when configuring named pools"
            );
        }
        if let Some(file) = file {
            anyhow::ensure!(
                !file.trim().is_empty(),
                "DUCKFLEET_POOLS_FILE must name a file"
            );
            let path = PathBuf::from(file);
            let source =
                std::fs::read_to_string(&path).context("reading configured pool registry file")?;
            return Ok((Self::from_json(&source)?, Some(path)));
        }
        if let Some(source) = inline {
            return Ok((Self::from_json(&source)?, None));
        }
        let urls = std::env::var("DUCKFLEET_WORKER_URLS").context(
            "configure DUCKFLEET_POOLS_FILE, DUCKFLEET_POOLS_JSON, or legacy DUCKFLEET_WORKER_URLS",
        )?;
        let endpoints: Vec<_> = urls
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_owned)
            .collect();
        anyhow::ensure!(
            !endpoints.is_empty(),
            "DUCKFLEET_WORKER_URLS must list at least one worker endpoint"
        );
        let source = json!({"default_pool":"small", "pools":{"small":{
            "cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":endpoints,
            "max_sessions_per_worker":default_sessions()
        }}});
        let mut registry = Self::from_json(&source.to_string())?;
        registry.legacy = true;
        registry.refresh_revision()?;
        Ok((registry, None))
    }

    pub fn from_json(source: &str) -> Result<Self> {
        let config: Configuration =
            serde_json::from_str(source).context("invalid pool registry JSON")?;
        anyhow::ensure!(
            valid_pool_name(&config.default_pool),
            "invalid default pool name"
        );
        anyhow::ensure!(
            config.pools.contains_key(&config.default_pool),
            "default pool is not configured"
        );
        let mut endpoints = HashSet::new();
        let mut pools = BTreeMap::new();
        for (name, configured) in config.pools {
            anyhow::ensure!(
                valid_pool_name(&name),
                "pool names must be lowercase DNS labels starting with a letter, at most 40 characters"
            );
            anyhow::ensure!(
                (1..=1024).contains(&configured.max_sessions_per_worker),
                "max_sessions_per_worker must be between 1 and 1024"
            );
            let resources = Resources::parse(
                &configured.cpu.to_string(),
                &configured.memory,
                &configured.disk,
            )
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("invalid allocation for pool {name}"))?;
            let members = configured
                .endpoints
                .iter()
                .map(|raw| {
                    let endpoint = endpoint(raw)?;
                    anyhow::ensure!(
                        endpoints.insert(endpoint.clone()),
                        "a worker endpoint is listed more than once in the pool registry"
                    );
                    Ok(endpoint)
                })
                .collect::<Result<Vec<_>>>()?;
            pools.insert(
                name,
                Pool {
                    resources,
                    endpoints: members,
                    max_sessions_per_worker: configured.max_sessions_per_worker,
                },
            );
        }
        let mut registry = Self {
            default_pool: config.default_pool,
            pools,
            revision: String::new(),
            legacy: false,
        };
        registry.refresh_revision()?;
        Ok(registry)
    }

    fn refresh_revision(&mut self) -> Result<()> {
        let canonical = serde_json::to_vec(
            &json!({"default_pool":self.default_pool,"pools":self.pools,"legacy":self.legacy}),
        )?;
        let hash = ring::digest::digest(&ring::digest::SHA256, &canonical);
        self.revision = hash
            .as_ref()
            .iter()
            .fold(String::with_capacity(64), |mut value, byte| {
                let _ = write!(value, "{byte:02x}");
                value
            });
        Ok(())
    }

    pub fn resolve(&self, request: &ComputeRequest) -> Result<Placement, ApiError> {
        let resolved = match request.selection()? {
            Selection::Isolated(resources) => Placement {
                mode: Mode::Isolated,
                pool: None,
                resources,
            },
            Selection::Persistent { pool, minimum } => {
                let name = pool.unwrap_or_else(|| self.default_pool.clone());
                let configured = self
                    .pools
                    .get(&name)
                    .ok_or_else(|| ApiError::bad(format!("unknown persistent pool {name:?}")))?;
                if !minimum.satisfied_by(configured.resources) {
                    return Err(ApiError::bad(format!(
                        "pool {name:?} does not satisfy the requested minimum resources"
                    )));
                }
                Placement {
                    mode: Mode::Persistent,
                    pool: Some(name),
                    resources: configured.resources,
                }
            }
        };
        request.validate_resolved(&resolved)?;
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> serde_json::Value {
        json!({"default_pool":"interactive","pools":{
            "small":{"cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":["http://small:8080"]},
            "interactive":{"cpu":2,"memory":"512Mi","disk":"4Gi","endpoints":["http://interactive:8080"],"max_sessions_per_worker":10},
            "same_size":{"cpu":1,"memory":"1Gi","disk":"2Gi","endpoints":[]}
        }})
    }
    fn config() -> serde_json::Value {
        let mut value = configuration();
        let same = value["pools"]
            .as_object_mut()
            .unwrap()
            .remove("same_size")
            .unwrap();
        value["pools"]["same-size"] = same;
        value
    }
    fn request(value: serde_json::Value) -> ComputeRequest {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn pools_resolve_without_size_inference_or_invented_minima() {
        let registry = PoolRegistry::from_json(&config().to_string()).unwrap();
        let default = registry.resolve(&ComputeRequest::default()).unwrap();
        assert_eq!(default.pool.as_deref(), Some("interactive"));
        assert_eq!(default.resources.memory_bytes, 512 * 1024 * 1024);
        let minimum = registry
            .resolve(&request(json!({"mode":"persistent","cpu":2})))
            .unwrap();
        assert_eq!(minimum, default);
        assert!(
            registry
                .resolve(&request(json!({"pool":"small","cpu":2})))
                .is_err()
        );
        assert!(
            registry
                .resolve(&request(json!({"pool":"missing"})))
                .is_err()
        );
        let isolated = registry.resolve(&request(json!({"cpu":2}))).unwrap();
        assert_eq!(isolated.mode, Mode::Isolated);
        assert_eq!(isolated.resources.memory_bytes, 1024 * 1024 * 1024);
        let unavailable = registry
            .resolve(&request(json!({"pool":"same-size"})))
            .unwrap();
        assert_eq!(unavailable.pool.as_deref(), Some("same-size"));
        assert!(registry.pools["same-size"].endpoints.is_empty());
    }

    #[test]
    fn registry_rejects_invalid_configuration_and_duplicate_endpoint_aliases() {
        let original = config();
        for (key, value) in [
            ("cpu", json!(0)),
            ("memory", json!("128Mi")),
            ("disk", json!("512Mi")),
            ("max_sessions_per_worker", json!(1025)),
        ] {
            let mut changed = original.clone();
            changed["pools"]["small"][key] = value;
            assert!(
                PoolRegistry::from_json(&changed.to_string()).is_err(),
                "accepted {key}"
            );
        }
        let mut duplicate = original.clone();
        duplicate["pools"]["interactive"]["endpoints"] = json!(["http://small:8080/"]);
        assert!(PoolRegistry::from_json(&duplicate.to_string()).is_err());
        for endpoint in [
            "file:///tmp/worker",
            "http://token@worker:8080",
            "http://worker:8080/prefix",
            "http://worker:8080?token=secret",
        ] {
            let mut changed = original.clone();
            changed["pools"]["small"]["endpoints"] = json!([endpoint]);
            assert!(PoolRegistry::from_json(&changed.to_string()).is_err());
        }
        assert!(PoolRegistry::from_json(&configuration().to_string()).is_err());
        let mut unknown_default = original;
        unknown_default["default_pool"] = json!("missing");
        assert!(PoolRegistry::from_json(&unknown_default.to_string()).is_err());
    }

    #[test]
    fn registry_revision_uses_normalized_configuration_but_retains_pool_identity() {
        let first = PoolRegistry::from_json(&config().to_string()).unwrap();
        let mut equivalent = config();
        equivalent["pools"]["small"]["memory"] = json!("1024Mi");
        equivalent["pools"]["small"]["endpoints"] = json!(["http://small:8080/"]);
        let second = PoolRegistry::from_json(&equivalent.to_string()).unwrap();
        assert_eq!(first.revision, second.revision);
        equivalent["default_pool"] = json!("small");
        assert_ne!(
            first.revision,
            PoolRegistry::from_json(&equivalent.to_string())
                .unwrap()
                .revision
        );
    }
}
