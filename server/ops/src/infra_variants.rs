use crate::{aws_simple, chart, util};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub fn helm_values(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: helm-values <default|foreign>");
    }
    let registry = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/acme-analytics";
    let value = match args[0].as_str() {
        "default" => json!({
            "workerImage":format!("797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev/worker@sha256:{}", "a".repeat(64)),
            "dbtImage":format!("797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev/dbt@sha256:{}", "a".repeat(64)),
        }),
        "foreign" => json!({
            "workerImage":format!("{registry}/worker@sha256:{}", "b".repeat(64)),
            "dbtImage":format!("{registry}/dbt@sha256:{}", "b".repeat(64)),
            "bucket":"acme-ducklake-eu-west-1","region":"eu-west-1","secretName":"acme-duckfleet",
        }),
        _ => bail!("unknown values fixture"),
    };
    println!("{value}");
    Ok(())
}

pub fn deploy_overlay(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: deploy-overlay <output.json>");
    }
    let overlay = aws_simple::overlay(&["10.42.3.17/32".into(), "10.42.9.4/32".into()], &[443]);
    ensure!(
        overlay["isolatedCompute"]["enabled"] == true,
        "isolated compute disabled"
    );
    ensure!(
        overlay["isolatedCompute"]["apiServerCidrs"] == json!(["10.42.3.17/32", "10.42.9.4/32"]),
        "CIDRs not sorted"
    );
    ensure!(
        overlay["isolatedCompute"]["readySeconds"].as_u64().unwrap() >= 300,
        "cold-node wait too short"
    );
    ensure!(
        overlay["isolatedCompute"]["provisioningSeconds"]
            .as_u64()
            .unwrap()
            >= 120,
        "provisioning grace too short"
    );
    ensure!(
        overlay["isolatedCompute"]["provisioningSeconds"].as_u64()
            <= overlay["isolatedCompute"]["readySeconds"].as_u64(),
        "provisioning grace exceeds ready budget"
    );
    ensure!(
        overlay["nodeSelector"]["karpenter.sh/nodepool"] == "duckfleet"
            && overlay["nodeSelector"]["eks.amazonaws.com/compute-type"].is_null(),
        "pool pin must null-delete compute-type"
    );
    ensure!(
        overlay["tolerations"][0]["key"] == "duckfleet.io/dedicated",
        "dedicated taint not tolerated"
    );
    util::write_json(&args[0], &overlay)
}

fn documents(path: &str) -> Result<Vec<Value>> {
    let documents = util::yaml_documents(&std::fs::read_to_string(path)?)?;
    chart::validate_chart(&documents)?;
    Ok(documents)
}

pub fn chart_scaled(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: chart-scaled <rendered.yaml>");
    }
    let documents = documents(&args[0])?;
    let workers: BTreeMap<_, _> = documents
        .iter()
        .filter(|document| {
            document["kind"] == "Deployment"
                && util::text(document["metadata"].get("name")).starts_with("worker-")
        })
        .map(|worker| (util::text(worker["metadata"].get("name")), worker))
        .collect();
    ensure!(
        workers.keys().copied().collect::<BTreeSet<_>>()
            == BTreeSet::from(["worker-small", "worker-small-1"]),
        "scaled worker names changed"
    );
    let mut identities = BTreeSet::new();
    for (name, worker) in &workers {
        let env = util::env_map(worker["spec"]["template"]["spec"]["containers"][0].get("env"));
        ensure!(
            identities.insert(env["DUCKFLEET_WORKER_ID"].clone()),
            "worker identities collide"
        );
        if *name == "worker-small" {
            ensure!(
                worker["spec"]["selector"]["matchLabels"]
                    == json!({"app":"duckfleet-worker","size":"small"}),
                "index zero immutable selector changed"
            );
        } else {
            ensure!(
                worker["spec"]["selector"]["matchLabels"]["worker-index"] == "1",
                "scaled selector missing index"
            );
        }
    }
    let gateway = documents
        .iter()
        .find(|document| {
            document["kind"] == "Deployment" && document["metadata"]["name"] == "server"
        })
        .unwrap();
    let env = util::env_map(gateway["spec"]["template"]["spec"]["containers"][0].get("env"));
    let mut urls: Vec<String> = env["DUCKFLEET_WORKER_URLS"]
        .split(',')
        .map(str::to_owned)
        .collect();
    urls.sort();
    ensure!(
        urls == ["http://worker-small-1:8080", "http://worker-small:8080"],
        "gateway endpoints are incomplete"
    );
    for service in documents.iter().filter(|document| {
        document["kind"] == "Service"
            && util::text(document["metadata"].get("name")).starts_with("worker-")
    }) {
        ensure!(
            service["spec"]["selector"].get("worker-index").is_some(),
            "worker Service can balance across siblings mid-session"
        );
    }
    Ok(())
}

pub fn chart_autoscaling(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: chart-autoscaling <rendered.yaml>");
    }
    let documents = documents(&args[0])?;
    for (name, desired) in [
        ("worker-pool-interactive-0", 1),
        ("worker-pool-interactive-1", 0),
    ] {
        let worker = documents
            .iter()
            .find(|document| {
                document["kind"] == "Deployment" && document["metadata"]["name"] == name
            })
            .with_context(|| format!("missing {name}"))?;
        ensure!(
            worker["spec"]["replicas"] == desired,
            "{name} starts at the wrong capacity"
        );
    }
    Ok(())
}

pub fn chart_flip(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: chart-flip <rendered.yaml>");
    }
    let documents = documents(&args[0])?;
    let worker = documents
        .iter()
        .find(|document| {
            document["kind"] == "Deployment" && document["metadata"]["name"] == "worker-small"
        })
        .context("missing small worker")?;
    let pod = &worker["spec"]["template"]["spec"];
    ensure!(
        pod["containers"][0]["resources"]["requests"]["cpu"] == "1",
        "worker request changed"
    );
    ensure!(
        pod["nodeSelector"]["karpenter.sh/nodepool"] == "duckfleet"
            && pod["nodeSelector"]
                .get("eks.amazonaws.com/compute-type")
                .is_none(),
        "worker pin blocks Karpenter"
    );
    ensure!(
        util::array(pod.get("tolerations"))
            .iter()
            .any(|toleration| toleration["key"] == "duckfleet.io/dedicated"),
        "taint not tolerated"
    );
    let template = documents
        .iter()
        .find(|document| {
            document["kind"] == "ConfigMap"
                && document["metadata"]["name"] == "duckfleet-isolated-pod"
        })
        .unwrap();
    let template = util::read_json_from_string(util::text(template["data"].get("pod.json")))?;
    let isolated = &template["spec"];
    ensure!(
        isolated["nodeSelector"]["karpenter.sh/nodepool"] == "duckfleet"
            && isolated["nodeSelector"]
                .get("eks.amazonaws.com/compute-type")
                .is_none(),
        "isolated pod has impossible compute-type selector"
    );
    ensure!(
        util::object(isolated.get("nodeSelector").unwrap())
            .values()
            .all(|value| !value.is_null()),
        "isolated pod has a null selector"
    );
    Ok(())
}

pub fn boundary_disjoint(args: Vec<String>) -> Result<()> {
    if args.len() != 2 {
        bail!("usage: boundary-disjoint <release.yaml> <boundary.yaml>");
    }
    let identify = |path: &str| -> Result<BTreeSet<(String, String)>> {
        Ok(util::yaml_documents(&std::fs::read_to_string(path)?)?
            .into_iter()
            .filter(|document| !document.is_null())
            .map(|document| {
                (
                    util::text(document.get("kind")).into(),
                    util::text(document["metadata"].get("name")).into(),
                )
            })
            .collect())
    };
    let release = identify(&args[0])?;
    let boundary = identify(&args[1])?;
    let both: Vec<_> = release.intersection(&boundary).collect();
    ensure!(
        both.is_empty(),
        "claimed by both release and boundary: {both:?}"
    );
    ensure!(
        release.contains(&("ServiceAccount".into(), "duckfleet-gateway".into())),
        "release does not create gateway identity"
    );
    ensure!(
        boundary.contains(&("RoleBinding".into(), "duckfleet-isolated-pods".into())),
        "boundary does not bind gateway identity"
    );
    Ok(())
}
