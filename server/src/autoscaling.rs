//! Deterministic bounded persistent-pool scaling decisions, separate from Kubernetes effects.
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct PoolPolicy {
    pub min_workers: u16,
    pub max_workers: u16,
    pub max_cpu: u16,
    pub max_memory_bytes: u64,
    pub queue_wait_target_ms: u64,
    pub scale_up_cooldown_seconds: u64,
    pub scale_down_cooldown_seconds: u64,
    pub idle_seconds: u64,
    pub paused: bool,
}

impl Default for PoolPolicy {
    fn default() -> Self {
        Self {
            min_workers: 1,
            max_workers: 2,
            max_cpu: 4,
            max_memory_bytes: 8 << 30,
            queue_wait_target_ms: 5_000,
            scale_up_cooldown_seconds: 30,
            scale_down_cooldown_seconds: 300,
            idle_seconds: 300,
            paused: false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Observation {
    pub workers: u16,
    pub cpu: u16,
    pub memory_bytes: u64,
    pub oldest_queued_ms: u64,
    pub active: bool,
    pub seconds_since_scale: u64,
    pub seconds_idle: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Decision {
    Hold(&'static str),
    ScaleUp(u16, &'static str),
    ScaleDown(u16, &'static str),
}

pub(crate) fn decide(policy: &PoolPolicy, observed: Observation) -> Decision {
    if observed.workers < policy.min_workers {
        return Decision::ScaleUp(policy.min_workers, "minimum_capacity");
    }
    if policy.paused {
        return Decision::Hold("operator_paused");
    }
    let additional = observed.workers.saturating_add(1);
    let within = additional <= policy.max_workers
        && observed.cpu <= policy.max_cpu
        && observed.memory_bytes <= policy.max_memory_bytes;
    if observed.oldest_queued_ms > 0
        && observed.oldest_queued_ms >= policy.queue_wait_target_ms
        && observed.seconds_since_scale >= policy.scale_up_cooldown_seconds
    {
        if within {
            return Decision::ScaleUp(additional, "queue_wait_target_exceeded");
        }
        return Decision::Hold("resource_ceiling");
    }
    if observed.workers > policy.min_workers
        && !observed.active
        && observed.seconds_idle >= policy.idle_seconds
        && observed.seconds_since_scale >= policy.scale_down_cooldown_seconds
    {
        return Decision::ScaleDown(observed.workers - 1, "idle_cooldown_elapsed");
    }
    Decision::Hold("stable_or_cooldown")
}

pub(crate) fn spawn(
    app: crate::gateway::Gateway,
    token: std::sync::Arc<str>,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    let source = std::env::var("DUCKFLEET_AUTOSCALING_JSON").unwrap_or_else(|_| "{}".into());
    let policies: std::collections::BTreeMap<String, PoolPolicy> = serde_json::from_str(&source)?;
    if policies.is_empty() {
        return Ok(None);
    }
    let kube = crate::kube::Kube::from_service_account()?;
    Ok(Some(tokio::spawn(async move {
        let mut transitions: std::collections::BTreeMap<
            String,
            (std::time::Instant, std::time::Instant),
        > = Default::default();
        let client = reqwest::Client::new();
        loop {
            let registry = app.registry();
            let fleet = app.fleet_snapshot(&registry).await;
            for (pool_name, policy) in &policies {
                let Some(pool) = registry.pools.get(pool_name) else {
                    tracing::warn!(pool = pool_name, "autoscaling pool is not configured");
                    continue;
                };
                if policy.min_workers > policy.max_workers
                    || policy.max_workers as usize > pool.endpoints.len()
                    || policy.max_cpu < pool.resources.cpu
                    || policy.max_memory_bytes < pool.resources.memory_bytes
                {
                    tracing::error!(
                        pool = pool_name,
                        "autoscaling policy exceeds the configured worker envelope"
                    );
                    continue;
                }
                let reported: Vec<_> = fleet
                    .reported
                    .iter()
                    .filter(|worker| worker["pool"].as_str() == Some(pool_name.as_str()))
                    .collect();
                let observed_workers = reported.len() as u16;
                let active = reported.iter().any(|worker| {
                    worker["active_queries"].as_u64().unwrap_or(0) > 0
                        || worker["open_sessions"].as_u64().unwrap_or(0) > 0
                });
                let oldest = reported
                    .iter()
                    .filter_map(|worker| worker["oldest_queued_ms"].as_u64())
                    .max()
                    .unwrap_or(0);
                let (last_scale, idle_since) = transitions
                    .entry(pool_name.clone())
                    .or_insert_with(|| (std::time::Instant::now(), std::time::Instant::now()));
                if active {
                    *idle_since = std::time::Instant::now();
                }
                let decision = decide(
                    policy,
                    Observation {
                        workers: observed_workers,
                        cpu: pool
                            .resources
                            .cpu
                            .saturating_mul(observed_workers.saturating_add(1)),
                        memory_bytes: pool
                            .resources
                            .memory_bytes
                            .saturating_mul(u64::from(observed_workers.saturating_add(1))),
                        oldest_queued_ms: oldest,
                        active,
                        seconds_since_scale: last_scale.elapsed().as_secs(),
                        seconds_idle: idle_since.elapsed().as_secs(),
                    },
                );
                let scaling_down = matches!(&decision, Decision::ScaleDown(_, _));
                let (desired, reason) = match decision {
                    Decision::Hold(reason) => {
                        tracing::info!(
                            pool = pool_name,
                            workers = observed_workers,
                            reason,
                            "autoscaling decision"
                        );
                        continue;
                    }
                    Decision::ScaleUp(desired, reason) | Decision::ScaleDown(desired, reason) => {
                        (desired, reason)
                    }
                };
                let index = if scaling_down {
                    desired
                } else {
                    desired.saturating_sub(1)
                };
                let name = deployment_name(pool_name, index);
                let deployment = match kube.deployment(&name).await {
                    Ok(deployment) => deployment,
                    Err(error) => {
                        tracing::error!(
                            pool = pool_name,
                            deployment = name,
                            error = format!("{error:#}"),
                            "autoscaling observation failed"
                        );
                        continue;
                    }
                };
                let current = deployment["spec"]["replicas"].as_u64().unwrap_or(0) as u16;
                if !transition_is_current(
                    current,
                    observed_workers,
                    desired,
                    scaling_down,
                    pool.endpoints.len() as u16,
                ) {
                    tracing::warn!(
                        pool = pool_name,
                        current,
                        observed = observed_workers,
                        desired,
                        "stale autoscaling observation retained"
                    );
                    continue;
                }
                if scaling_down {
                    let endpoint = format!("http://{name}:8080/v1/drain");
                    match client.post(endpoint).bearer_auth(&token).send().await {
                        Ok(response) if response.status().is_success() => {}
                        _ => {
                            tracing::warn!(
                                pool = pool_name,
                                deployment = name,
                                "scale-down drain was not acknowledged"
                            );
                            continue;
                        }
                    }
                }
                let replicas = if scaling_down { 0 } else { 1 };
                match kube.scale_deployment(&name, replicas).await {
                    Ok(_) => {
                        *last_scale = std::time::Instant::now();
                        tracing::info!(
                            pool = pool_name,
                            deployment = name,
                            desired,
                            reason,
                            "autoscaling capacity changed"
                        );
                    }
                    Err(error) => tracing::error!(
                        pool = pool_name,
                        deployment = name,
                        desired,
                        error = format!("{error:#}"),
                        "autoscaling provisioning failed"
                    ),
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    })))
}

fn transition_is_current(
    current: u16,
    observed: u16,
    desired: u16,
    scaling_down: bool,
    endpoints: u16,
) -> bool {
    let expected = if scaling_down {
        desired + 1
    } else {
        desired - 1
    };
    let starting_standby = !scaling_down && desired > 1 && current == 0 && observed <= expected;
    desired <= endpoints && (starting_standby || (current == expected && observed == expected))
}

fn deployment_name(pool: &str, index: u16) -> String {
    if pool == "small" {
        if index == 0 {
            "worker-small".into()
        } else {
            format!("worker-small-{index}")
        }
    } else {
        format!("worker-pool-{pool}-{index}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation() -> Observation {
        Observation {
            workers: 1,
            cpu: 1,
            memory_bytes: 1 << 30,
            oldest_queued_ms: 10_000,
            active: true,
            seconds_since_scale: 400,
            seconds_idle: 0,
        }
    }

    #[test]
    fn queue_pressure_is_bounded_by_workers_resources_and_cooldowns() {
        let policy = PoolPolicy::default();
        assert_eq!(
            decide(&policy, observation()),
            Decision::ScaleUp(2, "queue_wait_target_exceeded")
        );
        assert_eq!(
            decide(
                &policy,
                Observation {
                    workers: 2,
                    ..observation()
                }
            ),
            Decision::Hold("resource_ceiling")
        );
        assert_eq!(
            decide(
                &policy,
                Observation {
                    seconds_since_scale: 1,
                    ..observation()
                }
            ),
            Decision::Hold("stable_or_cooldown")
        );
        assert_eq!(
            decide(
                &policy,
                Observation {
                    cpu: 5,
                    ..observation()
                }
            ),
            Decision::Hold("resource_ceiling")
        );
    }

    #[test]
    fn an_empty_queue_does_not_satisfy_a_zero_wait_target() {
        let policy = PoolPolicy {
            queue_wait_target_ms: 0,
            ..PoolPolicy::default()
        };
        assert_eq!(
            decide(
                &policy,
                Observation {
                    oldest_queued_ms: 0,
                    ..observation()
                }
            ),
            Decision::Hold("stable_or_cooldown")
        );
    }

    #[test]
    fn scale_down_requires_idle_windows_and_preserves_the_minimum() {
        let policy = PoolPolicy::default();
        let mut observed = observation();
        observed.workers = 2;
        observed.active = false;
        observed.oldest_queued_ms = 0;
        assert_eq!(
            decide(&policy, observed.clone()),
            Decision::Hold("stable_or_cooldown")
        );
        observed.seconds_idle = 400;
        assert_eq!(
            decide(&policy, observed.clone()),
            Decision::ScaleDown(1, "idle_cooldown_elapsed")
        );
        observed.workers = 1;
        assert_eq!(
            decide(&policy, observed),
            Decision::Hold("stable_or_cooldown")
        );
    }

    #[test]
    fn a_zero_replica_standby_can_start_but_partial_transitions_wait() {
        assert!(transition_is_current(0, 0, 2, false, 2));
        assert!(transition_is_current(1, 1, 2, false, 2));
        assert!(!transition_is_current(1, 0, 2, false, 2));
        assert!(transition_is_current(0, 1, 2, false, 2));
        assert!(!transition_is_current(0, 2, 2, false, 2));
        assert!(transition_is_current(2, 2, 1, true, 2));
        assert!(!transition_is_current(0, 0, 1, true, 2));
        assert!(!transition_is_current(0, 0, 3, false, 2));
    }

    #[test]
    fn pause_never_removes_minimum_capacity() {
        let policy = PoolPolicy {
            paused: true,
            ..PoolPolicy::default()
        };
        let mut observed = observation();
        observed.workers = 0;
        assert_eq!(
            decide(&policy, observed.clone()),
            Decision::ScaleUp(1, "minimum_capacity")
        );
        observed.workers = 2;
        observed.active = false;
        observed.seconds_idle = 999;
        assert_eq!(decide(&policy, observed), Decision::Hold("operator_paused"));
    }
}
