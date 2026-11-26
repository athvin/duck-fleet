use crate::util;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub fn clone_job(source: &Value, name: &str, role: &str) -> Result<Value> {
    ensure!(
        source["metadata"]["annotations"]["duckfleet.io/platform-schema"] == "8",
        "Current migration template predates schema 7; stop compute and apply the desired chart/image first"
    );
    ensure!(
        matches!(role, "migrate" | "platform-grants"),
        "unsupported migration role"
    );
    let mut spec = source["spec"].clone();
    if let Some(containers) = spec["template"]["spec"]["containers"].as_array_mut() {
        for container in containers {
            if let Some(env) = container["env"].as_array_mut() {
                for item in env {
                    if item["name"] == "DUCKFLEET_ROLE" {
                        item["value"] = json!(role);
                    }
                }
            }
        }
    }
    let object = spec
        .as_object_mut()
        .context("migration spec must be an object")?;
    object.remove("selector");
    object.remove("manualSelector");
    let metadata = &mut spec["template"]["metadata"];
    metadata
        .as_object_mut()
        .unwrap()
        .remove("creationTimestamp");
    if let Some(labels) = metadata["labels"].as_object_mut() {
        for label in [
            "controller-uid",
            "job-name",
            "batch.kubernetes.io/controller-uid",
            "batch.kubernetes.io/job-name",
        ] {
            labels.remove(label);
        }
    }
    spec["ttlSecondsAfterFinished"] = json!(86400);
    Ok(json!({
        "apiVersion":"batch/v1","kind":"Job",
        "metadata":{"name":name,"labels":{"app":"duckfleet-platform-migrate"},"annotations":{"duckfleet.io/platform-schema":"8"}},
        "spec":spec,
    }))
}

pub fn migration_clone(args: Vec<String>) -> Result<()> {
    if args.len() != 3 {
        bail!("usage: migration-clone <template.json> <name> <role>");
    }
    let source = util::read_json(&args[0])?;
    println!(
        "{}",
        serde_json::to_string(&clone_job(&source, &args[1], &args[2])?)?
    );
    Ok(())
}

pub fn migration_deadline(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: migration-deadline <template.json>");
    }
    let source = util::read_json(&args[0])?;
    println!(
        "{}",
        source["spec"]["activeDeadlineSeconds"]
            .as_u64()
            .context("activeDeadlineSeconds is required")?
            + 30
    );
    Ok(())
}

pub fn api_egress_config(args: Vec<String>) -> Result<()> {
    if args.len() != 3 {
        bail!("usage: api-egress-config <namespace> <service.json> <endpoints.json>");
    }
    let service = util::read_json(&args[1])?;
    let endpoints = util::read_json(&args[2])?;
    let mut cidrs = BTreeSet::new();
    let mut ports = BTreeSet::new();
    for subset in util::array(endpoints.get("subsets")) {
        for address in util::array(subset.get("addresses")) {
            cidrs.insert(format!("{}/32", util::text(address.get("ip"))));
        }
        for port in util::array(subset.get("ports")) {
            ports.insert(port["port"].as_u64().context("endpoint port missing")?);
        }
    }
    let cluster_ip = service["spec"]["clusterIP"]
        .as_str()
        .context("Service clusterIP missing")?;
    cidrs.insert(format!("{cluster_ip}/32"));
    for port in util::array(service["spec"].get("ports")) {
        ports.insert(port["port"].as_u64().context("Service port missing")?);
    }
    ensure!(
        !cidrs.is_empty() && !ports.is_empty(),
        "the kubernetes Service resolves to no endpoints"
    );
    println!(
        "{}",
        serde_json::to_string(&json!({
            "apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"duckfleet-api-egress","namespace":args[0]},
            "data":{"cidrs":serde_json::to_string(&cidrs)?,"ports":serde_json::to_string(&ports)?},
        }))?
    );
    Ok(())
}

pub fn external_postgres(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: external-postgres <infra/kind/external-postgres.yaml>");
    }
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let source: Value = serde_json::from_str(&input)?;
    let env = source["spec"]["template"]["spec"]["containers"][0]["env"].clone();
    let mut documents = util::yaml_documents(&std::fs::read_to_string(&args[0])?)?;
    for document in &mut documents {
        if document["kind"] == "Deployment" {
            document["spec"]["template"]["spec"]["containers"][0]["env"] = env.clone();
        }
    }
    print!("{}", util::yaml_string(&documents)?);
    Ok(())
}

pub fn boundary_summary(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let documents = util::yaml_documents_from_reader(std::io::stdin())?;
    for document in documents.into_iter().filter(|document| !document.is_null()) {
        println!(
            "  {:<13}{}",
            util::text(document.get("kind")).to_ascii_lowercase(),
            util::text(document["metadata"].get("name"))
        );
    }
    Ok(())
}

pub fn check_role_grant(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let role = util::yaml_documents_from_reader(std::io::stdin())?
        .into_iter()
        .next()
        .context("missing Role")?;
    let granted: BTreeSet<String> = util::array(role.get("rules"))
        .iter()
        .filter(|rule| util::array(rule.get("resources")).contains(&json!("pods")))
        .flat_map(|rule| util::array(rule.get("verbs")))
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let required: BTreeSet<String> = ["create", "get", "list", "watch", "delete"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    ensure!(
        required.is_subset(&granted),
        "the pod grant is missing {:?}; it has {granted:?}",
        required.difference(&granted).collect::<Vec<_>>()
    );
    Ok(())
}

pub fn report_nodepool(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let pool = util::yaml_documents_from_reader(std::io::stdin())?
        .into_iter()
        .next()
        .context("missing NodePool")?;
    let spec = &pool["spec"];
    let sizes = util::array(spec["template"]["spec"].get("requirements"))
        .iter()
        .find(|requirement| util::text(requirement.get("key")).ends_with("instance-size"))
        .context("instance-size requirement missing")?["values"]
        .clone();
    println!(
        "node pool: sizes {sizes}, cpu ceiling {}, taints {}",
        spec["limits"]["cpu"], spec["template"]["spec"]["taints"]
    );
    Ok(())
}

pub fn chart_secret_name(args: Vec<String>) -> Result<()> {
    util::reject_extra(&args, 0)?;
    let values = util::read_yaml(util::root().join("infra/helm/duckfleet/values.yaml"))?;
    println!(
        "{}",
        values["secretName"]
            .as_str()
            .context("secretName missing")?
    );
    Ok(())
}

pub fn random_id(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let short = util::flag(&mut args, "--short");
    let token = util::flag(&mut args, "--token");
    util::reject_extra(&args, 0)?;
    let value = if token {
        util::random_hex(32)?
    } else {
        uuid::Uuid::new_v4().simple().to_string()
    };
    println!("{}", if short { &value[..12] } else { &value });
    Ok(())
}

pub fn free_ports(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: free-ports <count>");
    }
    let count = args[0].parse::<usize>()?;
    ensure!((1..=16).contains(&count), "count must be 1..16");
    let listeners: Vec<_> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<_>>()?;
    println!(
        "{}",
        listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    );
    Ok(())
}

pub async fn await_health(args: Vec<String>) -> Result<()> {
    let ports: BTreeSet<u16> = args
        .iter()
        .map(|port| port.parse())
        .collect::<std::result::Result<_, _>>()?;
    ensure!(!ports.is_empty(), "at least one port is required");
    let client = reqwest::Client::new();
    let mut pending = ports;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !pending.is_empty() && std::time::Instant::now() < deadline {
        let mut ready = BTreeSet::new();
        for port in &pending {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/healthz"))
                .timeout(std::time::Duration::from_secs(1))
                .send()
                .await
                && response.status() == reqwest::StatusCode::OK
            {
                ready.insert(*port);
            }
        }
        pending.retain(|port| !ready.contains(port));
        if !pending.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
    ensure!(
        pending.is_empty(),
        "gateway port-forwards did not become ready: {pending:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_migration_clones_configuration_without_old_job_identity() {
        let source = json!({
            "metadata":{"annotations":{"duckfleet.io/platform-schema":"8"}},
            "spec":{"selector":{"matchLabels":{"controller-uid":"old"}},"manualSelector":false,"activeDeadlineSeconds":390,"backoffLimit":0,
                "template":{"metadata":{"labels":{"app":"duckfleet-platform-migrate","controller-uid":"old","job-name":"old","batch.kubernetes.io/controller-uid":"old","batch.kubernetes.io/job-name":"old"}},
                    "spec":{"restartPolicy":"Never","containers":[{"name":"migrate","env":[{"name":"DUCKFLEET_ROLE","value":"migrate"}]}]}}}
        });
        let job = clone_job(&source, "new-job", "platform-grants").unwrap();
        assert_eq!(job["metadata"]["name"], "new-job");
        assert!(job["spec"].get("selector").is_none());
        assert_eq!(
            job["spec"]["template"]["metadata"]["labels"],
            json!({"app":"duckfleet-platform-migrate"})
        );
        assert_eq!(
            job["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"],
            "platform-grants"
        );
        assert_eq!(job["spec"]["ttlSecondsAfterFinished"], 86400);
    }

    #[test]
    fn api_egress_includes_service_and_endpoints() {
        let directory = tempfile::tempdir().unwrap();
        let service = directory.path().join("service.json");
        let endpoints = directory.path().join("endpoints.json");
        util::write_json(
            &service,
            &json!({"spec":{"clusterIP":"10.96.0.1","ports":[{"port":443}]}}),
        )
        .unwrap();
        util::write_json(
            &endpoints,
            &json!({"subsets":[{"addresses":[{"ip":"172.20.0.4"}],"ports":[{"port":6443}]}]}),
        )
        .unwrap();
        assert!(
            api_egress_config(vec![
                "duckfleet-dev".into(),
                service.to_string_lossy().into_owned(),
                endpoints.to_string_lossy().into_owned()
            ])
            .is_ok()
        );
    }
}
