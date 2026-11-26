use crate::{util, worker_resources};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn kubectl(
    context: &str,
    namespace: &str,
    arguments: &[String],
    check: bool,
) -> Result<std::process::Output> {
    let command = ["kubectl", "--context", context, "-n", namespace]
        .into_iter()
        .map(String::from)
        .chain(arguments.iter().cloned())
        .collect::<Vec<_>>();
    let output = util::run_status(command)?;
    if check && !output.status.success() {
        bail!("kubectl failed: {}", util::stderr_text(&output));
    }
    Ok(output)
}

fn required_options(args: &mut Vec<String>) -> Result<(String, String)> {
    let context = util::option(args, "--context").context("--context is required")?;
    let namespace = util::option(args, "--namespace").context("--namespace is required")?;
    Ok((context, namespace))
}

pub fn assert_limits(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let (context, namespace) = required_options(&mut args)?;
    util::reject_extra(&args, 0)?;
    let listed = kubectl(
        &context,
        &namespace,
        &[
            "get".into(),
            "pods".into(),
            "-l".into(),
            "app=duckfleet-worker".into(),
            "-o".into(),
            "json".into(),
        ],
        true,
    )?;
    let listed: Value = serde_json::from_slice(&listed.stdout)?;
    let workers: Vec<_> = util::array(listed.get("items"))
        .iter()
        .filter(|pod| {
            pod["metadata"]["labels"].get("duckfleet-compute").is_none()
                && pod["metadata"].get("deletionTimestamp").is_none()
        })
        .collect();
    let mut failures = Vec::new();
    if workers.is_empty() {
        failures.push("no persistent workers were found".to_owned());
    }
    for pod in workers {
        let name = util::text(pod["metadata"].get("name"));
        let container = util::array(pod["spec"].get("containers"))
            .iter()
            .find(|container| container["name"] == "worker")
            .context("worker container missing")?;
        let env = util::env_map(container.get("env"));
        let spec = &container["resources"];
        let cpu = env["DUCKFLEET_WORKER_CPU"].parse::<u32>()?;
        let memory = worker_resources::quantity_bytes(&env["DUCKFLEET_WORKER_MEMORY"])?;
        if let Err(error) = worker_resources::assert_worker_resources(
            spec,
            cpu,
            name,
            env.get("DUCKFLEET_WORKER_MEMORY").map(String::as_str),
            env.get("DUCKFLEET_WORKER_DISK").map(String::as_str),
        ) {
            failures.push(error.to_string());
        }
        let observed = kubectl(
            &context,
            &namespace,
            &[
                "exec".into(),
                name.into(),
                "-c".into(),
                "worker".into(),
                "--".into(),
                "sh".into(),
                "-c".into(),
                "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max".into(),
            ],
            true,
        )?;
        let observed = util::stdout_text(&observed);
        let values: Vec<u64> = observed
            .split_whitespace()
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()?;
        if values.len() != 3 {
            bail!("{name}: unexpected cgroup output");
        }
        let (quota, period, memory_max) = (values[0], values[1], values[2]);
        println!(
            "{}",
            serde_json::to_string(&json!({
                "pod":name,
                "size":pod["metadata"]["labels"].get("size"),
                "pool":env.get("DUCKFLEET_POOL"),
                "worker_id":env["DUCKFLEET_WORKER_ID"],
                "cpu_request":spec["requests"]["cpu"],
                "cpu_limit":spec["limits"]["cpu"],
                "memory_limit":spec["limits"]["memory"],
                "disk":env["DUCKFLEET_WORKER_DISK"],
                "cgroup_cpu_quota":quota,
                "cgroup_cpu_period":period,
                "cgroup_memory_max":memory_max,
            }))?
        );
        if quota != period * u64::from(cpu) {
            failures.push(format!("{name}: cpu.max {quota}/{period} is not {cpu} CPU"));
        }
        if memory_max != memory {
            failures.push(format!(
                "{name}: memory.max {memory_max} differs from declared {memory} bytes"
            ));
        }
        let request = worker_resources::cores(util::text(spec["requests"].get("cpu")))?;
        let limit = worker_resources::cores(util::text(spec["limits"].get("cpu")))?;
        if request <= 0.0 || request >= limit {
            failures.push(format!(
                "{name}: Kind CPU request must be positive and below its limit"
            ));
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    eprintln!("PASS: worker cgroups match independent allocations while CPU requests sit lower");
    Ok(())
}

pub fn validate_role_posture(pods: &[Value]) -> Result<BTreeMap<String, u64>> {
    let mut counts = BTreeMap::from([
        ("worker".to_owned(), 0_u64),
        ("gateway".to_owned(), 0),
        ("migrate".to_owned(), 0),
    ]);
    for pod in pods {
        let role = match util::text(pod["metadata"]["labels"].get("app")) {
            "duckfleet-worker" => "worker",
            "duckfleet-gateway" => "gateway",
            "duckfleet-platform-migrate" => "migrate",
            _ => continue,
        };
        let spec = &pod["spec"];
        ensure!(
            util::array(spec.get("volumes"))
                .iter()
                .all(|volume| !util::object(volume)
                    .keys()
                    .any(|key| key.contains("secret"))),
            "whole-Secret mount in compute"
        );
        for container in util::array(spec.get("containers")) {
            ensure!(
                container
                    .get("envFrom")
                    .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty)),
                "whole-Secret import in compute"
            );
            let names: std::collections::BTreeSet<_> = util::array(container.get("env"))
                .iter()
                .filter_map(|entry| entry["name"].as_str())
                .collect();
            let keys: BTreeMap<String, String> = util::array(container.get("env"))
                .iter()
                .filter_map(|entry| {
                    Some((
                        util::text(entry.get("name")).to_owned(),
                        entry["valueFrom"]["secretKeyRef"]["key"]
                            .as_str()?
                            .to_owned(),
                    ))
                })
                .collect();
            match role {
                "worker" => {
                    ensure!(
                        !names.contains("DUCKFLEET_TOKEN")
                            && !names.contains("DUCKFLEET_MAINTENANCE_DSN"),
                        "worker has privileged token"
                    );
                    let expected = BTreeMap::from([
                        ("PLATFORM_DSN".into(), "PLATFORM_WORKER_DSN".into()),
                        ("CATALOG_DSN".into(), "CATALOG_DSN".into()),
                        (
                            "DUCKFLEET_INTERNAL_TOKEN".into(),
                            "DUCKFLEET_INTERNAL_TOKEN".into(),
                        ),
                    ]);
                    ensure!(keys == expected, "worker credential set differs: {keys:?}");
                    ensure!(
                        !spec["automountServiceAccountToken"]
                            .as_bool()
                            .unwrap_or(false),
                        "SQL worker has API token"
                    );
                }
                "gateway" => {
                    let expected = BTreeMap::from([
                        ("PLATFORM_DSN".into(), "PLATFORM_GATEWAY_DSN".into()),
                        (
                            "DUCKFLEET_MAINTENANCE_DSN".into(),
                            "PLATFORM_MAINTENANCE_DSN".into(),
                        ),
                        ("DUCKFLEET_TOKEN".into(), "DUCKFLEET_TOKEN".into()),
                        (
                            "DUCKFLEET_INTERNAL_TOKEN".into(),
                            "DUCKFLEET_INTERNAL_TOKEN".into(),
                        ),
                    ]);
                    ensure!(keys == expected, "gateway credential set differs");
                }
                _ => {
                    ensure!(
                        keys == BTreeMap::from([(
                            "PLATFORM_DSN".into(),
                            "PLATFORM_MIGRATION_DSN".into()
                        )]),
                        "migration carries runtime secrets"
                    );
                }
            }
        }
        *counts.get_mut(role).unwrap() += 1;
    }
    ensure!(
        counts["worker"] > 0 && counts["gateway"] > 0,
        "expected live compute Pods"
    );
    Ok(counts)
}

pub fn assert_role_posture(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let (context, namespace) = required_options(&mut args)?;
    util::reject_extra(&args, 0)?;
    let output = kubectl(
        &context,
        &namespace,
        &["get".into(), "pods".into(), "-o".into(), "json".into()],
        true,
    )?;
    let pods: Value = serde_json::from_slice(&output.stdout)?;
    let counts = validate_role_posture(util::array(pods.get("items")))?;
    println!(
        "{}",
        serde_json::to_string(&json!({"checked_component_pods":counts}))?
    );
    Ok(())
}

fn probe(
    name: &str,
    label: &str,
    address: &str,
    image: &str,
    context: &str,
    namespace: &str,
) -> Result<(u32, String)> {
    kubectl(
        context,
        namespace,
        &[
            "delete".into(),
            "pod".into(),
            name.into(),
            "--ignore-not-found".into(),
            "--wait=true".into(),
        ],
        true,
    )?;
    let (label, value) = label
        .split_once('=')
        .context("probe label must be name=value")?;
    let manifest = json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"name":name,"labels":{(label):value}},
        "spec":{
            "restartPolicy":"Never","automountServiceAccountToken":false,
            "containers":[{
                "name":"probe","image":image,"imagePullPolicy":"Never",
                "command":["sh","-c",format!("curl -sS --max-time 5 -o /dev/null {address}; echo exit=$?")],
                "resources":{"requests":{"cpu":"10m","memory":"32Mi"},"limits":{"cpu":"100m","memory":"64Mi"}}
            }]
        }
    });
    util::run_with_stdin(
        [
            "kubectl",
            "--context",
            context,
            "--namespace",
            namespace,
            "apply",
            "-f",
            "-",
        ],
        &serde_json::to_string(&manifest)?,
    )?;
    for _ in 0..60 {
        let phase = kubectl(
            context,
            namespace,
            &[
                "get".into(),
                "pod".into(),
                name.into(),
                "-o".into(),
                "jsonpath={.status.phase}".into(),
            ],
            false,
        )?;
        if matches!(util::stdout_text(&phase).as_str(), "Succeeded" | "Failed") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    let logs = kubectl(context, namespace, &["logs".into(), name.into()], true)?;
    let logs = util::stdout_text(&logs);
    kubectl(
        context,
        namespace,
        &[
            "delete".into(),
            "pod".into(),
            name.into(),
            "--ignore-not-found".into(),
            "--wait=false".into(),
        ],
        true,
    )?;
    let code = logs
        .lines()
        .find_map(|line| line.strip_prefix("exit="))
        .map(str::parse)
        .transpose()?
        .context("probe produced no exit code")?;
    Ok((code, logs))
}

fn record_probe(
    evidence: &mut Value,
    name: &str,
    code: u32,
    logs: String,
    expected: &str,
) -> Result<()> {
    let verdict = if matches!(code, 7 | 28) {
        "blocked"
    } else if matches!(code, 0 | 35 | 60) {
        "connected"
    } else {
        "inconclusive"
    };
    evidence["probes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"probe":name,"curl_exit":code,"verdict":verdict,"expected":expected}));
    ensure!(
        verdict != "inconclusive",
        "{name}: curl exited {code}: {logs}"
    );
    ensure!(
        verdict == expected,
        "{name}: expected {expected}, got {verdict} (curl {code})"
    );
    Ok(())
}

pub fn check_networkpolicy(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let (context, namespace) = required_options(&mut args)?;
    let image = util::option(&mut args, "--image").context("--image is required")?;
    let release = util::option(&mut args, "--release").context("--release is required")?;
    let evidence_path = util::option(&mut args, "--evidence").context("--evidence is required")?;
    util::reject_extra(&args, 0)?;
    let address = kubectl(
        &context,
        "default",
        &[
            "get".into(),
            "endpoints".into(),
            "kubernetes".into(),
            "-o".into(),
            "jsonpath={.subsets[0].addresses[0].ip}".into(),
        ],
        true,
    )?;
    let port = kubectl(
        &context,
        "default",
        &[
            "get".into(),
            "endpoints".into(),
            "kubernetes".into(),
            "-o".into(),
            "jsonpath={.subsets[0].ports[0].port}".into(),
        ],
        true,
    )?;
    let target = format!(
        "https://{}:{}/version",
        util::stdout_text(&address).trim(),
        util::stdout_text(&port).trim()
    );
    let mut evidence = json!({"api_server":target,"probes":[]});
    let (code, logs) = probe(
        "netpol-gateway",
        "app=duckfleet-gateway",
        &target,
        &image,
        &context,
        &namespace,
    )?;
    record_probe(
        &mut evidence,
        "gateway to the API server",
        code,
        logs,
        "connected",
    )?;
    let (code, logs) = probe(
        "netpol-worker",
        "app=duckfleet-worker",
        &target,
        &image,
        &context,
        &namespace,
    )?;
    record_probe(
        &mut evidence,
        "worker to the API server",
        code,
        logs,
        "blocked",
    )?;
    let policies = kubectl(
        &context,
        &namespace,
        &[
            "get".into(),
            "networkpolicy".into(),
            "-o".into(),
            "name".into(),
        ],
        true,
    )?;
    let policies = util::stdout_text(&policies);
    let policies: Vec<String> = policies.split_whitespace().map(str::to_owned).collect();
    evidence["policies_removed"] = json!(policies);
    kubectl(
        &context,
        &namespace,
        &["delete".into()]
            .into_iter()
            .chain(policies.clone())
            .collect::<Vec<_>>(),
        true,
    )?;
    let unpoliced = probe(
        "netpol-worker-unpoliced",
        "app=duckfleet-worker",
        &target,
        &image,
        &context,
        &namespace,
    );
    let restore = util::run([
        "kubectl",
        "--context",
        &context,
        "--namespace",
        &namespace,
        "apply",
        "-f",
        &release,
    ]);
    let (code, logs) = unpoliced?;
    restore?;
    record_probe(
        &mut evidence,
        "worker to the API server, policies removed",
        code,
        logs,
        "connected",
    )?;
    let (code, logs) = probe(
        "netpol-worker-again",
        "app=duckfleet-worker",
        &target,
        &image,
        &context,
        &namespace,
    )?;
    record_probe(
        &mut evidence,
        "worker to the API server, policies restored",
        code,
        logs,
        "blocked",
    )?;
    util::write_json(evidence_path, &evidence)?;
    println!(
        "PASS: NetworkPolicy is enforced — the gateway reaches the API server, a worker cannot, and removing the policy is what lets it through"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_posture_requires_scoped_keys() {
        let pods = vec![
            json!({"metadata":{"labels":{"app":"duckfleet-worker"}},"spec":{"containers":[{"env":[
                {"name":"PLATFORM_DSN","valueFrom":{"secretKeyRef":{"key":"PLATFORM_WORKER_DSN"}}},
                {"name":"CATALOG_DSN","valueFrom":{"secretKeyRef":{"key":"CATALOG_DSN"}}},
                {"name":"DUCKFLEET_INTERNAL_TOKEN","valueFrom":{"secretKeyRef":{"key":"DUCKFLEET_INTERNAL_TOKEN"}}}
            ]}]}}),
            json!({"metadata":{"labels":{"app":"duckfleet-gateway"}},"spec":{"containers":[{"env":[
                {"name":"PLATFORM_DSN","valueFrom":{"secretKeyRef":{"key":"PLATFORM_GATEWAY_DSN"}}},
                {"name":"DUCKFLEET_MAINTENANCE_DSN","valueFrom":{"secretKeyRef":{"key":"PLATFORM_MAINTENANCE_DSN"}}},
                {"name":"DUCKFLEET_TOKEN","valueFrom":{"secretKeyRef":{"key":"DUCKFLEET_TOKEN"}}},
                {"name":"DUCKFLEET_INTERNAL_TOKEN","valueFrom":{"secretKeyRef":{"key":"DUCKFLEET_INTERNAL_TOKEN"}}}
            ]}]}}),
        ];
        let counts = validate_role_posture(&pods).unwrap();
        assert_eq!(counts["worker"], 1);
        assert_eq!(counts["gateway"], 1);
    }
}
