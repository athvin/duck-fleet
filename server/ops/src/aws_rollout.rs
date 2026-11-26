use crate::util;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub fn unschedulable_reason(pod: &Value) -> Option<String> {
    util::array(pod["status"].get("conditions"))
        .iter()
        .find(|condition| {
            condition["type"] == "PodScheduled"
                && condition["status"] == "False"
                && condition["reason"] == "Unschedulable"
        })
        .map(|condition| {
            condition["message"]
                .as_str()
                .unwrap_or("no scheduler message")
                .to_owned()
        })
}

pub fn deployments_pending(deployments: &[Value]) -> Vec<String> {
    let mut pending = Vec::new();
    for deployment in deployments {
        let desired = deployment["spec"]["replicas"].as_i64().unwrap_or(1);
        let status = &deployment["status"];
        let ready = status["readyReplicas"].as_i64().unwrap_or(0);
        let name = util::text(deployment["metadata"].get("name"));
        if status["observedGeneration"].as_i64().unwrap_or(0)
            < deployment["metadata"]["generation"].as_i64().unwrap_or(0)
        {
            pending.push(format!("{name} waiting for its current generation"));
        } else if ready != desired {
            pending.push(format!("{name} {ready}/{desired} ready"));
        } else if status["updatedReplicas"].as_i64().unwrap_or(0) != desired
            || status["replicas"].as_i64().unwrap_or(0) != desired
        {
            pending.push(format!("{name} waiting for its current replicas"));
        }
    }
    pending
}

pub fn required_migrations(deployments: &[Value]) -> BTreeSet<String> {
    deployments
        .iter()
        .filter(|deployment| deployment["spec"]["replicas"].as_i64().unwrap_or(1) > 0)
        .filter_map(|deployment| {
            deployment["metadata"]["annotations"]
                .get("duckfleet.io/migration-job")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

pub fn migration_state(deployments: &[Value], jobs: &[Value]) -> (&'static str, String) {
    let by_name: BTreeMap<_, _> = jobs
        .iter()
        .map(|job| (util::text(job["metadata"].get("name")), job))
        .collect();
    let mut pending = Vec::new();
    for name in required_migrations(deployments) {
        let Some(job) = by_name.get(name.as_str()) else {
            pending.push(format!("migration {name} has not been created"));
            continue;
        };
        let conditions: BTreeMap<_, _> = util::array(job["status"].get("conditions"))
            .iter()
            .filter(|condition| condition["status"] == "True")
            .map(|condition| (util::text(condition.get("type")), condition))
            .collect();
        if let Some(failure) = conditions.get("Failed") {
            return (
                "failed",
                format!(
                    "migration {name} failed: {} {}",
                    util::text(failure.get("reason")),
                    util::text(failure.get("message"))
                )
                .trim()
                .to_owned(),
            );
        }
        if !conditions.contains_key("Complete") {
            pending.push(format!("migration {name} has not completed"));
        }
    }
    if pending.is_empty() {
        ("ready", "migrations complete".into())
    } else {
        ("waiting", pending.join("; "))
    }
}

pub fn completed_migrations_to_prune(deployments: &[Value], jobs: &[Value]) -> Vec<String> {
    if required_migrations(deployments).is_empty()
        || migration_state(deployments, jobs).0 != "ready"
    {
        return Vec::new();
    }
    let current: BTreeSet<_> = deployments
        .iter()
        .filter_map(|deployment| {
            deployment["metadata"]["annotations"]
                .get("duckfleet.io/migration-job")
                .and_then(Value::as_str)
        })
        .collect();
    let mut retired = Vec::new();
    for job in jobs {
        let metadata = &job["metadata"];
        let name = util::text(metadata.get("name"));
        if metadata["labels"]["app"] != "duckfleet-platform-migrate"
            || current.contains(name)
            || metadata.get("deletionTimestamp").is_some()
        {
            continue;
        }
        let status = &job["status"];
        let conditions: BTreeSet<_> = util::array(status.get("conditions"))
            .iter()
            .filter(|condition| condition["status"] == "True")
            .map(|condition| util::text(condition.get("type")))
            .collect();
        if conditions.contains("Complete")
            && !conditions.contains("Failed")
            && status["active"].as_i64().unwrap_or(0) == 0
        {
            retired.push(name.to_owned());
        }
    }
    retired.sort();
    retired
}

pub fn evaluate(
    deployments: &[Value],
    pods: &[Value],
    first_seen: &mut BTreeMap<String, f64>,
    now: f64,
    grace_seconds: f64,
    jobs: &[Value],
    statefulsets: &[Value],
) -> (&'static str, String) {
    let (migration, detail) = migration_state(deployments, jobs);
    if migration == "failed" {
        return (migration, detail);
    }
    let required = required_migrations(deployments);
    for pod in pods {
        if matches!(
            pod["status"]["phase"].as_str(),
            Some("Succeeded" | "Failed")
        ) {
            continue;
        }
        if pod["metadata"]["labels"]["app"] == "duckfleet-platform-migrate" {
            let required_owner = util::array(pod["metadata"].get("ownerReferences"))
                .iter()
                .any(|owner| {
                    owner["kind"] == "Job" && required.contains(util::text(owner.get("name")))
                });
            if !required_owner {
                continue;
            }
        }
        let uid = util::text(pod["metadata"].get("uid")).to_owned();
        let Some(reason) = unschedulable_reason(pod) else {
            first_seen.remove(&uid);
            continue;
        };
        let waited = now - *first_seen.entry(uid).or_insert(now);
        if waited >= grace_seconds {
            return (
                "failed",
                format!(
                    "{} stayed unschedulable for {}s: {reason}",
                    util::text(pod["metadata"].get("name")),
                    waited as u64
                ),
            );
        }
    }
    if migration == "waiting" {
        return (migration, detail);
    }
    let mut pending = deployments_pending(deployments);
    for statefulset in statefulsets {
        let desired = statefulset["spec"]["replicas"].as_i64().unwrap_or(1);
        let ready = statefulset["status"]["readyReplicas"].as_i64().unwrap_or(0);
        if ready != desired
            || statefulset["status"]["observedGeneration"]
                .as_i64()
                .unwrap_or(0)
                < statefulset["metadata"]["generation"].as_i64().unwrap_or(0)
        {
            pending.push(format!(
                "{} {ready}/{desired} ready",
                util::text(statefulset["metadata"].get("name"))
            ));
        }
    }
    if pending.is_empty() {
        (
            "ready",
            "the current migration and every desired workload are ready".into(),
        )
    } else {
        ("waiting", pending.join("; "))
    }
}

fn objects(kubectl: &[String], kind: &str) -> Result<Vec<Value>> {
    let command = kubectl
        .iter()
        .cloned()
        .chain(["get".into(), kind.into(), "-o".into(), "json".into()])
        .collect::<Vec<_>>();
    let value: Value = serde_json::from_slice(&util::run(command)?.stdout)?;
    Ok(util::array(value.get("items")).to_vec())
}

pub fn run(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let namespace = util::option(&mut args, "--namespace").context("--namespace is required")?;
    let context = util::option(&mut args, "--context").context("--context is required")?;
    let timeout = util::option(&mut args, "--timeout-seconds")
        .unwrap_or_else(|| "600".into())
        .parse::<u64>()?;
    let grace = util::option(&mut args, "--grace-seconds")
        .unwrap_or_else(|| "90".into())
        .parse::<u64>()?;
    let poll = util::option(&mut args, "--poll-seconds")
        .unwrap_or_else(|| "5".into())
        .parse::<u64>()?;
    let prune = util::flag(&mut args, "--prune-migrations");
    util::reject_extra(&args, 0)?;
    let kubectl = vec![
        "kubectl".into(),
        "--context".into(),
        context,
        "-n".into(),
        namespace,
    ];
    let started = std::time::Instant::now();
    let mut first_seen = BTreeMap::new();
    loop {
        let deployments = objects(&kubectl, "deployments")?;
        let jobs = objects(&kubectl, "jobs")?;
        let pods = objects(&kubectl, "pods")?;
        let statefulsets = objects(&kubectl, "statefulsets")?;
        let elapsed = started.elapsed().as_secs_f64();
        let (state, message) = evaluate(
            &deployments,
            &pods,
            &mut first_seen,
            elapsed,
            grace as f64,
            &jobs,
            &statefulsets,
        );
        match state {
            "ready" => {
                if prune {
                    let names = completed_migrations_to_prune(&deployments, &jobs);
                    if !names.is_empty() {
                        let command = kubectl
                            .iter()
                            .cloned()
                            .chain(["delete".into(), "job".into()])
                            .chain(names.iter().cloned())
                            .chain([
                                "--ignore-not-found".into(),
                                "--wait=false".into(),
                                "--cascade=background".into(),
                            ])
                            .collect::<Vec<_>>();
                        util::run(command)?;
                        println!(
                            "Pruned {} superseded successful migration Job(s)",
                            names.len()
                        );
                    }
                }
                println!("PASS: {message}");
                return Ok(());
            }
            "failed" => {
                for name in required_migrations(&deployments) {
                    let output = util::run_status(
                        kubectl
                            .iter()
                            .cloned()
                            .chain([
                                "logs".into(),
                                format!("job/{name}"),
                                "--all-containers".into(),
                                "--tail=100".into(),
                            ])
                            .collect::<Vec<_>>(),
                    )?;
                    eprint!("{}", util::stdout_text(&output));
                    eprint!("{}", util::stderr_text(&output));
                }
                bail!("Deploy abandoned: {message}");
            }
            _ if elapsed >= timeout as f64 => {
                bail!("Release did not converge within {timeout}s: {message}")
            }
            _ => {
                println!("waiting: {message}");
                std::thread::sleep(std::time::Duration::from_secs(poll));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deployment(name: &str, desired: i64, ready: i64) -> Value {
        json!({"metadata":{"name":name},"spec":{"replicas":desired},"status":{"readyReplicas":ready,"updatedReplicas":ready,"replicas":desired}})
    }

    fn pod(name: &str, uid: &str, reason: Option<&str>) -> Value {
        json!({"metadata":{"name":name,"uid":uid},"status":{"conditions":[if let Some(reason) = reason {
            json!({"type":"PodScheduled","status":"False","reason":"Unschedulable","message":reason})
        } else {
            json!({"type":"PodScheduled","status":"True"})
        }]}})
    }

    #[test]
    fn readiness_requires_current_replicas_and_allows_suspend() {
        let ready = vec![deployment("worker-small", 1, 1), deployment("server", 1, 1)];
        assert_eq!(
            evaluate(
                &ready,
                &[pod("worker-small-1", "a", None)],
                &mut BTreeMap::new(),
                0.0,
                90.0,
                &[],
                &[]
            )
            .0,
            "ready"
        );
        let mut pending = vec![deployment("worker-small", 1, 0)];
        pending.extend(ready);
        let result = evaluate(
            &pending,
            &[pod("worker-small-1", "a", None)],
            &mut BTreeMap::new(),
            0.0,
            90.0,
            &[],
            &[],
        );
        assert_eq!(result.0, "waiting");
        assert!(result.1.contains("worker-small 0/1 ready"));
        assert_eq!(
            evaluate(
                &[deployment("worker-medium", 0, 0)],
                &[],
                &mut BTreeMap::new(),
                0.0,
                90.0,
                &[],
                &[]
            )
            .0,
            "ready"
        );
    }

    #[test]
    fn unschedulable_pods_get_a_bounded_grace() {
        let pods = vec![pod(
            "worker-medium-1",
            "m",
            Some("0/3 nodes are available: insufficient cpu"),
        )];
        let deployments = vec![deployment("worker-medium", 1, 0)];
        let mut first_seen = BTreeMap::new();
        assert_eq!(
            evaluate(&deployments, &pods, &mut first_seen, 0.0, 90.0, &[], &[]).0,
            "waiting"
        );
        assert_eq!(
            evaluate(&deployments, &pods, &mut first_seen, 89.0, 90.0, &[], &[]).0,
            "waiting"
        );
        let result = evaluate(&deployments, &pods, &mut first_seen, 90.0, 90.0, &[], &[]);
        assert_eq!(result.0, "failed");
        assert!(result.1.contains("insufficient cpu"));
    }

    #[test]
    fn migration_completion_and_pruning_preserve_current_and_failed_runs() {
        let mut server = deployment("server", 1, 1);
        server["metadata"]["annotations"] = json!({"duckfleet.io/migration-job":"current"});
        let old = json!({"metadata":{"name":"old","labels":{"app":"duckfleet-platform-migrate"}},"status":{"conditions":[{"type":"Complete","status":"True"}]}});
        let current = json!({"metadata":{"name":"current","labels":{"app":"duckfleet-platform-migrate"}},"status":{"conditions":[{"type":"Complete","status":"True"}]}});
        assert_eq!(
            migration_state(&[server.clone()], &[old.clone(), current.clone()]).0,
            "ready"
        );
        assert_eq!(
            completed_migrations_to_prune(&[server.clone()], &[old, current]),
            vec!["old"]
        );
        assert_eq!(
            completed_migrations_to_prune(&[server], &[]),
            Vec::<String>::new()
        );
    }
}
