use crate::util;
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;

const PLATFORM_SCHEMA: u32 = 9;
const SCHEMA_MARKER: &str = "duckfleet.io/platform-schema";
const GATEWAY: &str = "duckfleet-gateway";
const WORKER: &str = "duckfleet-worker";

pub fn schema_matches(value: Option<&Value>) -> Result<bool> {
    let Some(value) = value else { return Ok(false) };
    let value = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .unwrap_or_default();
    if !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u32>()? > PLATFORM_SCHEMA
    {
        bail!(
            "cannot deploy schema {PLATFORM_SCHEMA} over schema {value}; binary rollback is unsupported"
        );
    }
    Ok(value == PLATFORM_SCHEMA.to_string())
}

pub fn compute_deployments(items: &[Value]) -> Vec<&Value> {
    items
        .iter()
        .filter(|item| {
            matches!(
                item["spec"]["template"]["metadata"]["labels"]["app"].as_str(),
                Some(GATEWAY | WORKER)
            )
        })
        .collect()
}

pub fn compute_pods(items: &[Value]) -> Vec<&Value> {
    items
        .iter()
        .filter(|item| {
            matches!(
                item["metadata"]["labels"]["app"].as_str(),
                Some(GATEWAY | WORKER)
            ) || item["metadata"]["labels"]["duckfleet-compute"] == "isolated"
        })
        .collect()
}

pub fn check_kube_state(deployments: &[Value], pods: &[Value]) -> Result<()> {
    let mut blocked = Vec::new();
    for deployment in compute_deployments(deployments) {
        let marker = deployment["spec"]["template"]["metadata"]["annotations"].get(SCHEMA_MARKER);
        if !schema_matches(marker)? && deployment["spec"]["replicas"].as_i64().unwrap_or(1) != 0 {
            blocked.push(format!(
                "deployment/{}",
                util::text(deployment["metadata"].get("name"))
            ));
        }
    }
    for pod in compute_pods(pods) {
        if !schema_matches(pod["metadata"]["annotations"].get(SCHEMA_MARKER))? {
            blocked.push(format!("pod/{}", util::text(pod["metadata"].get("name"))));
        }
    }
    ensure!(
        blocked.is_empty(),
        "schema-changing upgrade requires stopped compute: {}; run scripts/stop-compute.sh kube, then retry the deployment",
        blocked.join(", ")
    );
    Ok(())
}

fn kube_objects(kubectl: &[String], kind: &str) -> Result<Vec<Value>> {
    let output = util::run(
        kubectl
            .iter()
            .cloned()
            .chain(["get".into(), kind.into(), "-o".into(), "json".into()])
            .collect::<Vec<_>>(),
    )?;
    let value: Value = serde_json::from_slice(&output.stdout)?;
    Ok(util::array(value.get("items")).to_vec())
}

fn stop_kube(kubectl: &[String], timeout: u64) -> Result<()> {
    let deployment_objects = kube_objects(kubectl, "deployments")?;
    let deployments = compute_deployments(&deployment_objects);
    for role in [GATEWAY, WORKER] {
        for deployment in deployments.iter().filter(|deployment| {
            deployment["spec"]["template"]["metadata"]["labels"]["app"] == role
        }) {
            let name = util::text(deployment["metadata"].get("name"));
            println!(
                "stopping deployment/{name} (replicas={})",
                deployment["spec"]["replicas"].as_i64().unwrap_or(1)
            );
            util::run(
                kubectl
                    .iter()
                    .cloned()
                    .chain([
                        "patch".into(),
                        format!("deployment/{name}"),
                        "--type=strategic".into(),
                        "--patch={\"spec\":{\"replicas\":0}}".into(),
                    ])
                    .collect::<Vec<_>>(),
            )?;
        }
        util::run(
            kubectl
                .iter()
                .cloned()
                .chain([
                    "delete".into(),
                    "pods".into(),
                    "-l".into(),
                    format!("app={role}"),
                    "--ignore-not-found".into(),
                    "--wait=true".into(),
                    format!("--timeout={timeout}s"),
                ])
                .collect::<Vec<_>>(),
        )?;
    }
    util::run(
        kubectl
            .iter()
            .cloned()
            .chain([
                "delete".into(),
                "pods".into(),
                "-l".into(),
                "duckfleet-compute=isolated".into(),
                "--ignore-not-found".into(),
                "--wait=true".into(),
                format!("--timeout={timeout}s"),
            ])
            .collect::<Vec<_>>(),
    )?;
    let remaining_objects = kube_objects(kubectl, "deployments")?;
    let remaining = compute_deployments(&remaining_objects);
    ensure!(
        !remaining
            .iter()
            .any(|item| item["spec"]["replicas"].as_i64().unwrap_or(1) != 0),
        "a compute controller was restarted during shutdown; compute remains unmanaged, do not migrate"
    );
    ensure!(
        compute_pods(&kube_objects(kubectl, "pods")?).is_empty(),
        "compute pods remain after shutdown; do not migrate"
    );
    Ok(())
}

fn compose_containers() -> Result<Vec<Value>> {
    let ids = util::output_text([
        "docker",
        "compose",
        "--profile",
        "*",
        "ps",
        "--all",
        "--quiet",
    ])?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let output = util::run(
        ["docker", "inspect"]
            .into_iter()
            .map(String::from)
            .chain(ids.split_whitespace().map(str::to_owned))
            .collect::<Vec<_>>(),
    )?;
    Ok(serde_json::from_slice(&output.stdout)?)
}

pub fn container_role(container: &Value) -> Option<&'static str> {
    let env: std::collections::BTreeMap<_, _> = util::array(container["Config"].get("Env"))
        .iter()
        .filter_map(|entry| entry.as_str()?.split_once('='))
        .collect();
    if env.get("DUCKFLEET_ROLE") == Some(&"gateway") {
        return Some(GATEWAY);
    }
    if env.contains_key("DUCKFLEET_WORKER_ID") {
        return Some(WORKER);
    }
    None
}

pub fn check_compose_state(containers: &[Value]) -> Result<()> {
    for container in containers {
        if container_role(container).is_none() || container["State"]["Running"] != true {
            continue;
        }
        let marker = container["Config"]["Labels"].get(SCHEMA_MARKER);
        if !schema_matches(marker)? {
            bail!(
                "schema-changing upgrade requires stopped compute: {}; run scripts/stop-compute.sh compose, then retry",
                container["Name"]
                    .as_str()
                    .unwrap_or_else(|| util::text(container.get("Id")))
            );
        }
    }
    Ok(())
}

fn stop_compose() -> Result<()> {
    let containers = compose_containers()?;
    for role in [GATEWAY, WORKER] {
        let ids: Vec<String> = containers
            .iter()
            .filter(|container| {
                container_role(container) == Some(role) && container["State"]["Running"] == true
            })
            .map(|container| util::text(container.get("Id")).to_owned())
            .collect();
        if !ids.is_empty() {
            util::run(
                ["docker", "stop", "-t", "630"]
                    .into_iter()
                    .map(String::from)
                    .chain(ids)
                    .collect::<Vec<_>>(),
            )?;
        }
    }
    ensure!(
        !compose_containers()?
            .iter()
            .any(|container| container_role(container).is_some()
                && container["State"]["Running"] == true),
        "compute restarted during shutdown; do not migrate"
    );
    Ok(())
}

pub fn run(args: Vec<String>) -> Result<()> {
    if args.len() < 2
        || !matches!(args[0].as_str(), "check" | "stop")
        || !matches!(args[1].as_str(), "compose" | "kube")
    {
        bail!(
            "usage: compute-lifecycle <check|stop> <compose|kube> [--context CONTEXT] [--namespace NAMESPACE] [--timeout SECONDS]"
        );
    }
    let action = args[0].clone();
    let mode = args[1].clone();
    let mut args = args[2..].to_vec();
    let context = util::option(&mut args, "--context")
        .or_else(|| std::env::var("DUCKFLEET_KUBE_CONTEXT").ok());
    let namespace = util::option(&mut args, "--namespace")
        .or_else(|| std::env::var("DUCKFLEET_NAMESPACE").ok());
    let timeout = util::option(&mut args, "--timeout")
        .unwrap_or_else(|| "180".into())
        .parse::<u64>()?;
    ensure!(
        (1..=1800).contains(&timeout),
        "timeout must be 1..1800 seconds"
    );
    util::reject_extra(&args, 0)?;
    if mode == "compose" {
        if action == "stop" {
            stop_compose()?;
        } else {
            check_compose_state(&compose_containers()?)?;
        }
    } else {
        let context = context.context("set an explicit context")?;
        let namespace = namespace.context("set an explicit namespace")?;
        let kubectl = vec![
            "kubectl".into(),
            "--context".into(),
            context,
            "--namespace".into(),
            namespace,
        ];
        if action == "stop" {
            stop_kube(&kubectl, timeout)?;
        } else {
            check_kube_state(
                &kube_objects(&kubectl, "deployments")?,
                &kube_objects(&kubectl, "pods")?,
            )?;
        }
    }
    println!(
        "{}",
        if action == "stop" {
            "Compute stopped; PostgreSQL and migration Jobs were preserved. Restart explicitly after migration."
        } else {
            "Compute schema-upgrade preflight passed."
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_preflight_rejects_older_running_and_future_compute() {
        assert!(schema_matches(Some(&json!("9"))).unwrap());
        assert!(!schema_matches(Some(&json!("7"))).unwrap());
        assert!(schema_matches(Some(&json!("10"))).is_err());
        let deployment = json!({"metadata":{"name":"worker-small"},"spec":{"replicas":1,"template":{"metadata":{"labels":{"app":"duckfleet-worker"},"annotations":{"duckfleet.io/platform-schema":"7"}}}}});
        assert!(check_kube_state(&[deployment], &[]).is_err());
    }

    #[test]
    fn compute_filters_include_isolated_and_exclude_stateful_services() {
        let isolated =
            json!({"metadata":{"name":"isolated","labels":{"duckfleet-compute":"isolated"}}});
        let postgres = json!({"metadata":{"name":"postgres","labels":{"app":"postgres"}}});
        assert_eq!(compute_pods(&[isolated, postgres]).len(), 1);
    }
}
