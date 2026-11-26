use crate::{network::Network, util, worker_resources};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn admission_limits(overrides: &Value, defaults: Option<&Value>) -> Result<()> {
    let mut merged = defaults.cloned().unwrap_or_else(|| json!({}));
    for (key, value) in util::object(overrides) {
        merged[key] = value.clone();
    }
    let allowed: BTreeSet<&str> = [
        "max_queued_queries",
        "max_running_queries",
        "max_queued_per_principal",
        "max_running_per_principal",
    ]
    .into_iter()
    .collect();
    ensure!(
        merged
            .as_object()
            .is_some_and(|values| values.keys().all(|key| allowed.contains(key.as_str()))),
        "unknown admission limit"
    );
    let queued = merged["max_queued_queries"].as_i64().unwrap_or(256);
    let running = merged["max_running_queries"].as_i64().unwrap_or(16);
    let principal_queued = merged["max_queued_per_principal"].as_i64().unwrap_or(16);
    let principal_running = merged["max_running_per_principal"]
        .as_i64()
        .unwrap_or(running);
    for key in [
        "max_queued_queries",
        "max_running_queries",
        "max_queued_per_principal",
        "max_running_per_principal",
    ] {
        if merged.get(key).is_some_and(|value| !value.is_i64()) {
            bail!("{key} must be an integer");
        }
    }
    ensure!(
        (1..=65535).contains(&queued) && (1..=1024).contains(&running),
        "invalid pool admission bounds"
    );
    ensure!(
        (1..=queued).contains(&principal_queued) && (1..=running).contains(&principal_running),
        "invalid principal admission bounds"
    );
    Ok(())
}

fn worker_queue_limits(env: &BTreeMap<String, String>) -> Result<()> {
    for (key, minimum, maximum) in [
        ("DUCKFLEET_WORKER_MAX_QUEUED_QUERIES", 1_u64, 4096_u64),
        ("DUCKFLEET_WORKER_MAX_QUEUED_BYTES", 1_048_576, 268_435_456),
    ] {
        let value = env.get(key).with_context(|| format!("missing {key}"))?;
        ensure!(
            value.is_ascii() && value.bytes().all(|byte| byte.is_ascii_digit()),
            "{key} must be a decimal integer"
        );
        let value = value.parse::<u64>()?;
        ensure!(
            (minimum..=maximum).contains(&value),
            "{key} is outside its supported bounds"
        );
    }
    Ok(())
}

fn rate_pair(env: Option<&Value>) -> (String, String) {
    let env = util::env_map(env);
    (
        env.get("DUCKFLEET_RATE_CARD_VERSION")
            .cloned()
            .unwrap_or_default(),
        env.get("DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR")
            .cloned()
            .unwrap_or_default(),
    )
}

fn secret_keys(container: &Value) -> Result<BTreeMap<String, String>> {
    ensure!(
        !container
            .get("envFrom")
            .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty)),
        "whole-Secret imports expose privileged sibling keys"
    );
    Ok(util::array(container.get("env"))
        .iter()
        .filter_map(|entry| {
            let key = entry["valueFrom"]["secretKeyRef"]["key"].as_str()?;
            Some((util::text(entry.get("name")).to_owned(), key.to_owned()))
        })
        .collect())
}

pub fn check_chart(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: check-chart <rendered.yaml>");
    }
    let text = std::fs::read_to_string(&args[0])?;
    validate_chart(&util::yaml_documents(&text)?)?;
    println!("PASS: chart resource sizes, identity, isolation, private services, and scoped DNS");
    Ok(())
}

pub fn validate_chart(objects: &[Value]) -> Result<()> {
    let root = util::root();
    let query = std::fs::read_to_string(root.join("server/src/query.rs"))?;
    let max_statement = regex::Regex::new(r"MAX_EXECUTION_TIMEOUT: u64 = (\d+)")?
        .captures(&query)
        .and_then(|captures| captures[1].parse::<u64>().ok())
        .context("cannot read MAX_EXECUTION_TIMEOUT")?;
    let platform = std::fs::read_to_string(root.join("server/src/platform.rs"))?;
    let platform_schema = regex::Regex::new(r"SCHEMA_VERSION: i32 = (\d+)")?
        .captures(&platform)
        .map(|captures| captures[1].to_owned())
        .context("cannot read platform SCHEMA_VERSION")?;
    let objects: Vec<_> = objects
        .iter()
        .filter(|object| !object.is_null())
        .cloned()
        .collect();
    let by_name: BTreeMap<(String, String), Value> = objects
        .iter()
        .map(|object| {
            (
                (
                    util::text(object.get("kind")).to_owned(),
                    util::text(object["metadata"].get("name")).to_owned(),
                ),
                object.clone(),
            )
        })
        .collect();
    let persistent_workers: BTreeMap<String, Value> = by_name
        .iter()
        .filter(|((kind, name), _)| kind == "Deployment" && name.starts_with("worker-"))
        .map(|((_, name), object)| (name.clone(), object.clone()))
        .collect();
    for (name, worker) in &persistent_workers {
        ensure!(
            worker["spec"]["strategy"]["type"] == "Recreate",
            "{name} must use Recreate"
        );
        let pod = &worker["spec"]["template"]["spec"];
        let container = &pod["containers"][0];
        let declared = util::env_map(container.get("env"));
        let cpu = declared["DUCKFLEET_WORKER_CPU"].parse::<u32>()?;
        worker_resources::assert_worker_resources(
            &container["resources"],
            cpu,
            &format!("rendered {name}"),
            declared.get("DUCKFLEET_WORKER_MEMORY").map(String::as_str),
            declared.get("DUCKFLEET_WORKER_DISK").map(String::as_str),
        )?;
        ensure!(
            *name == format!("worker-{}", declared["DUCKFLEET_WORKER_ID"]),
            "worker identity and name disagree"
        );
        ensure!(
            declared["DUCKFLEET_POOL"]
                == util::text(
                    worker["spec"]["template"]["metadata"]["labels"].get("duckfleet-pool")
                ),
            "worker pool label disagrees"
        );
        ensure!(
            (1..=1024).contains(&declared["DUCKFLEET_WORKER_MAX_SESSIONS"].parse::<u64>()?),
            "invalid worker session limit"
        );
        ensure!(
            (1..=4096).contains(&declared["DUCKFLEET_WORKER_QUEUE_CAPACITY"].parse::<u64>()?),
            "invalid worker queue capacity"
        );
        worker_queue_limits(&declared)?;
        ensure!(
            worker["spec"]["template"]["metadata"]["annotations"]["duckfleet.io/platform-schema"]
                == platform_schema,
            "worker schema marker disagrees"
        );
        let tmp = util::array(pod.get("volumes"))
            .iter()
            .find(|volume| volume["name"] == "tmp")
            .context("worker has no tmp volume")?;
        ensure!(
            worker_resources::quantity_bytes(util::text(tmp["emptyDir"].get("sizeLimit")))?
                == worker_resources::quantity_bytes(&declared["DUCKFLEET_WORKER_DISK"])?,
            "tmp volume differs from declared disk"
        );
        ensure!(
            declared["DUCKFLEET_WORKER_CPU"]
                == util::text(container["resources"]["limits"].get("cpu")),
            "worker CPU declaration differs from limit"
        );
        ensure!(
            declared["DUCKFLEET_WORKER_MEMORY"]
                == util::text(container["resources"]["limits"].get("memory")),
            "worker memory declaration differs from limit"
        );
        ensure!(
            declared["DUCKFLEET_WORKER_DISK"]
                == util::text(container["resources"]["requests"].get("ephemeral-storage")),
            "worker disk declaration differs from request"
        );
        let authority = declared
            .get("DUCKFLEET_AUTHORITY")
            .map_or("writer", String::as_str);
        ensure!(
            matches!(authority, "reader" | "writer"),
            "invalid worker authority"
        );
        ensure!(
            pod["serviceAccountName"]
                == if authority == "reader" {
                    "duckfleet-reader"
                } else {
                    "duckfleet-worker"
                },
            "wrong worker service account"
        );
        if authority == "reader" {
            ensure!(
                declared.contains_key("CATALOG_READER_DSN")
                    && !declared.contains_key("CATALOG_DSN"),
                "reader has writer DSN"
            );
            ensure!(
                !declared
                    .keys()
                    .any(|key| key.starts_with("DUCKFLEET_RESULT_")),
                "reader has result-write configuration"
            );
        } else {
            ensure!(
                declared.contains_key("CATALOG_DSN")
                    && !declared.contains_key("CATALOG_READER_DSN"),
                "writer DSN configuration is incomplete"
            );
        }
        ensure!(
            declared["S3_AUTH_MODE"] == "pod_identity"
                && declared["AWS_EC2_METADATA_DISABLED"] == "true",
            "worker must use Pod Identity"
        );
        ensure!(
            !declared.contains_key("S3_SECRET_KEY") && !declared.contains_key("DUCKFLEET_ISOLATED"),
            "persistent worker has isolated/static credentials"
        );
        ensure!(
            pod["terminationGracePeriodSeconds"].as_u64().unwrap_or(30) > max_statement,
            "{name} cannot drain its longest statement"
        );
        let probe = &container["startupProbe"];
        let wait = declared["DUCKFLEET_CATALOG_WAIT_SECONDS"].parse::<u64>()?
            + declared["DUCKFLEET_PLATFORM_WAIT_SECONDS"].parse::<u64>()?;
        ensure!(
            wait <= probe["periodSeconds"].as_u64().unwrap_or(0)
                * probe["failureThreshold"].as_u64().unwrap_or(0),
            "startup probe can interrupt a supported catalog wait"
        );
    }

    let mut placements = BTreeMap::new();
    for object in &objects {
        let kind = util::text(object.get("kind"));
        ensure!(
            !matches!(
                kind,
                "ClusterRole"
                    | "ClusterRoleBinding"
                    | "NodePool"
                    | "Role"
                    | "RoleBinding"
                    | "ResourceQuota"
            ),
            "{kind} is a boundary object, not a release object"
        );
        if kind == "Service" {
            ensure!(
                object["spec"]["type"].as_str().unwrap_or("ClusterIP") == "ClusterIP",
                "Services must remain ClusterIP"
            );
        }
        if matches!(kind, "Deployment" | "StatefulSet" | "Job") {
            let pod = &object["spec"]["template"]["spec"];
            ensure!(
                pod["securityContext"]["runAsNonRoot"] == true,
                "pod must run non-root"
            );
            placements.insert(
                util::text(object["metadata"].get("name")).to_owned(),
                (
                    serde_json::to_string(pod.get("nodeSelector").unwrap_or(&Value::Null))?,
                    serde_json::to_string(pod.get("tolerations").unwrap_or(&Value::Null))?,
                ),
            );
            for container in util::array(pod.get("containers"))
                .iter()
                .chain(util::array(pod.get("initContainers")))
            {
                ensure!(
                    container["securityContext"]["allowPrivilegeEscalation"] == false,
                    "privilege escalation must be refused"
                );
                ensure!(
                    container["securityContext"]["capabilities"]["drop"] == json!(["ALL"]),
                    "all capabilities must be dropped"
                );
                ensure!(
                    container["resources"]["requests"]
                        .as_object()
                        .is_some_and(|values| !values.is_empty())
                        && container["resources"]["limits"]
                            .as_object()
                            .is_some_and(|values| !values.is_empty()),
                    "container resources must be bounded"
                );
            }
        }
    }
    let unique: BTreeSet<_> = placements.values().collect();
    ensure!(
        unique.len() == 1,
        "pods disagree about where they run: {placements:?}"
    );
    let (selector, tolerations) = unique.iter().next().context("release has no pods")?;
    let pinned = selector.contains("karpenter.sh/nodepool");
    let tolerated = tolerations != "null" && tolerations.contains("duckfleet.io/dedicated");
    ensure!(
        pinned == tolerated,
        "node-pool selector and taint toleration must agree"
    );

    let server = by_name
        .get(&("Deployment".into(), "server".into()))
        .context("missing gateway Deployment")?;
    let gateway_pod = &server["spec"]["template"]["spec"];
    ensure!(
        gateway_pod["terminationGracePeriodSeconds"]
            .as_u64()
            .unwrap_or(30)
            > max_statement,
        "gateway cannot drain its longest statement"
    );
    let gateway = util::array(gateway_pod.get("containers"))
        .iter()
        .find(|container| container["name"] == "gateway")
        .context("missing gateway container")?;
    let gateway_env = util::env_map(gateway.get("env"));
    ensure!(
        server["spec"]["template"]["metadata"]["annotations"]["duckfleet.io/platform-schema"]
            == platform_schema,
        "gateway schema marker disagrees"
    );
    let admission = util::read_json_from_string(
        gateway_env
            .get("DUCKFLEET_ADMISSION_LIMITS_JSON")
            .context("missing admission limits")?,
    )?;
    ensure!(
        util::object(&admission)
            .keys()
            .all(|key| matches!(key.as_str(), "default" | "isolated")),
        "unknown admission scope"
    );
    for limits in util::object(&admission).values() {
        admission_limits(limits, None)?;
    }
    ensure!(
        (1..=4096).contains(&gateway_env["DUCKFLEET_GATEWAY_MAX_SESSIONS"].parse::<u64>()?),
        "invalid gateway session limit"
    );

    let pool_config = by_name.get(&("ConfigMap".into(), "duckfleet-pools".into()));
    let mut worker_urls = Vec::new();
    if let Some(pool_config) = pool_config {
        ensure!(
            !gateway_env.contains_key("DUCKFLEET_WORKER_URLS"),
            "named pools and legacy endpoints are mutually exclusive"
        );
        ensure!(
            gateway_env.get("DUCKFLEET_POOLS_FILE").map(String::as_str)
                == Some("/etc/duckfleet-pools/pools.json"),
            "gateway pool file is not configured"
        );
        let config =
            util::read_json_from_string(util::text(pool_config["data"].get("pools.json")))?;
        let default_pool = util::text(config.get("default_pool"));
        ensure!(
            config["pools"].get(default_pool).is_some(),
            "default pool is absent"
        );
        for (pool_name, pool) in util::object(&config["pools"]) {
            admission_limits(
                pool.get("admission").unwrap_or(&Value::Null),
                admission.get("default"),
            )?;
            for url in util::array(pool.get("endpoints")) {
                let url = url.as_str().context("pool endpoint must be a string")?;
                let host = url
                    .strip_prefix("http://")
                    .unwrap_or(url)
                    .split(':')
                    .next()
                    .unwrap_or_default();
                let worker = persistent_workers
                    .get(host)
                    .with_context(|| format!("pool {pool_name} points at missing worker {host}"))?;
                let declared =
                    util::env_map(worker["spec"]["template"]["spec"]["containers"][0].get("env"));
                ensure!(
                    declared["DUCKFLEET_POOL"] == *pool_name,
                    "worker pool identity disagrees"
                );
                ensure!(
                    declared["DUCKFLEET_WORKER_CPU"].parse::<u64>()?
                        == pool["cpu"].as_u64().unwrap_or(0),
                    "pool CPU disagrees"
                );
                ensure!(
                    worker_resources::quantity_bytes(&declared["DUCKFLEET_WORKER_MEMORY"])?
                        == worker_resources::quantity_bytes(util::text(pool.get("memory")))?,
                    "pool memory disagrees"
                );
                ensure!(
                    worker_resources::quantity_bytes(&declared["DUCKFLEET_WORKER_DISK"])?
                        == worker_resources::quantity_bytes(util::text(pool.get("disk")))?,
                    "pool disk disagrees"
                );
                ensure!(
                    declared["DUCKFLEET_WORKER_MAX_SESSIONS"].parse::<u64>()?
                        == pool["max_sessions_per_worker"].as_u64().unwrap_or(0),
                    "pool session cap disagrees"
                );
                worker_urls.push(url.to_owned());
            }
        }
        let mounts: BTreeMap<_, _> = util::array(gateway.get("volumeMounts"))
            .iter()
            .map(|mount| (util::text(mount.get("name")), mount))
            .collect();
        ensure!(
            mounts["persistent-pools"]["readOnly"] == true
                && mounts["persistent-pools"].get("subPath").is_none(),
            "pool ConfigMap must be read-only and atomic"
        );
    } else {
        ensure!(
            !gateway_env.contains_key("DUCKFLEET_POOLS_FILE"),
            "pool file without pool ConfigMap"
        );
        worker_urls = gateway_env["DUCKFLEET_WORKER_URLS"]
            .split(',')
            .map(str::to_owned)
            .collect();
        ensure!(
            !worker_urls.is_empty() && worker_urls.iter().all(|url| !url.is_empty()),
            "legacy gateway needs worker endpoints"
        );
    }
    let services: BTreeSet<_> = by_name
        .keys()
        .filter(|(kind, _)| kind == "Service")
        .map(|(_, name)| name.clone())
        .collect();
    for url in &worker_urls {
        let host = url
            .strip_prefix("http://")
            .unwrap_or(url)
            .split(':')
            .next()
            .unwrap_or_default();
        ensure!(
            services.contains(host) && by_name.contains_key(&("Deployment".into(), host.into())),
            "gateway points at missing worker {host}"
        );
    }
    ensure!(
        worker_urls.iter().collect::<BTreeSet<_>>().len() == worker_urls.len()
            && worker_urls.len() == persistent_workers.len(),
        "worker endpoints must be unique and complete"
    );

    let mut cards = BTreeMap::new();
    cards.insert("server".to_owned(), rate_pair(gateway.get("env")));
    for (name, worker) in &persistent_workers {
        cards.insert(
            name.clone(),
            rate_pair(worker["spec"]["template"]["spec"]["containers"][0].get("env")),
        );
    }
    if let Some(template) = by_name.get(&("ConfigMap".into(), "duckfleet-isolated-pod".into())) {
        let session_pod =
            util::read_json_from_string(util::text(template["data"].get("pod.json")))?;
        ensure!(
            session_pod["metadata"]["annotations"]["duckfleet.io/platform-schema"]
                == platform_schema,
            "isolated schema marker disagrees"
        );
        worker_queue_limits(&util::env_map(
            session_pod["spec"]["containers"][0].get("env"),
        ))?;
        cards.insert(
            "isolated-pod".into(),
            rate_pair(session_pod["spec"]["containers"][0].get("env")),
        );
    }
    let rates: BTreeSet<_> = cards.values().collect();
    ensure!(rates.len() == 1, "rate cards disagree: {cards:?}");
    let (rate_version, rate_value) = rates.iter().next().unwrap();
    ensure!(
        !rate_version.is_empty()
            && rate_value.bytes().all(|byte| byte.is_ascii_digit())
            && rate_value.parse::<u64>()? >= 1,
        "invalid deployment rate card"
    );

    let server_ports: BTreeSet<_> = util::array(
        by_name
            .get(&("Service".into(), "server".into()))
            .context("missing server Service")?["spec"]
            .get("ports"),
    )
    .iter()
    .filter_map(|port| port["port"].as_u64())
    .collect();
    ensure!(
        (gateway_env.get("DUCKFLEET_PGWIRE").map(String::as_str) == Some("true"))
            == server_ports.contains(&5432),
        "pgwire listener and Service port disagree"
    );

    let migration_name = server["metadata"]["annotations"]
        .get("duckfleet.io/migration-job")
        .and_then(Value::as_str);
    let migrations: Vec<_> = objects
        .iter()
        .filter(|object| {
            object["kind"] == "Job"
                && object["metadata"]["labels"]["app"] == "duckfleet-platform-migrate"
        })
        .collect();
    if server["spec"]["replicas"].as_i64().unwrap_or(1) == 0 {
        ensure!(
            migration_name.is_none() && migrations.is_empty(),
            "a suspended release must not migrate"
        );
    } else {
        ensure!(
            migrations.len() == 1
                && Some(util::text(migrations[0]["metadata"].get("name"))) == migration_name,
            "desired migration Job is missing"
        );
        let migration = migrations[0];
        ensure!(
            migration["metadata"]["annotations"]["duckfleet.io/platform-schema"] == platform_schema,
            "migration schema marker disagrees"
        );
        ensure!(
            migration["metadata"]["annotations"]
                .get("helm.sh/hook")
                .is_none(),
            "migration hooks can deadlock fresh installs"
        );
        let pod = &migration["spec"]["template"]["spec"];
        ensure!(
            pod["serviceAccountName"] == "duckfleet-no-aws"
                && pod["automountServiceAccountToken"] == false,
            "migration identity is too broad"
        );
        ensure!(
            pod["restartPolicy"] == "Never" && migration["spec"]["backoffLimit"] == 0,
            "migration retry policy changed"
        );
        let migrate = &pod["containers"][0];
        ensure!(
            migrate["image"] == gateway["image"],
            "migration and gateway images differ"
        );
        let env = util::env_map(migrate.get("env"));
        let expected: BTreeSet<&str> = [
            "DUCKFLEET_ROLE",
            "PLATFORM_DSN",
            "DUCKFLEET_PLATFORM_WAIT_SECONDS",
            "DUCKFLEET_MIGRATION_TIMEOUT_SECONDS",
            "DUCKFLEET_PREPAID_MICRO_USD",
            "DUCKFLEET_MAX_JOB_MICRO_USD",
            "DUCKFLEET_NAMESPACE",
            "DUCKFLEET_DATABASE_ROLES_JSON",
            "DUCKFLEET_AUTHZ_DEFAULT_POOL",
            "DUCKFLEET_AUTHZ_DEFAULT_CPU",
            "DUCKFLEET_AUTHZ_DEFAULT_MEMORY",
            "DUCKFLEET_AUTHZ_DEFAULT_DISK",
        ]
        .into_iter()
        .collect();
        ensure!(
            env.keys().map(String::as_str).collect::<BTreeSet<_>>() == expected,
            "migration environment changed: {env:?}"
        );
        ensure!(env["DUCKFLEET_ROLE"] == "migrate", "migration role changed");
        for name in ["DUCKFLEET_PREPAID_MICRO_USD", "DUCKFLEET_MAX_JOB_MICRO_USD"] {
            ensure!(
                env[name].is_ascii()
                    && env[name].bytes().all(|byte| byte.is_ascii_digit())
                    && env[name].parse::<u64>()? > 0,
                "{name} must be a positive whole number"
            );
        }
        ensure!(
            migration["spec"]["activeDeadlineSeconds"]
                .as_u64()
                .unwrap_or(0)
                >= env["DUCKFLEET_PLATFORM_WAIT_SECONDS"].parse::<u64>()?
                    + env["DUCKFLEET_MIGRATION_TIMEOUT_SECONDS"].parse::<u64>()?,
            "migration deadline is too short"
        );
        ensure!(
            ["startupProbe", "readinessProbe", "livenessProbe"]
                .iter()
                .all(|key| migrate.get(*key).is_none()),
            "migration probes are invalid"
        );
    }
    ensure!(
        gateway_env.contains_key("DUCKFLEET_HISTORY_RETENTION_DAYS"),
        "gateway retention configuration missing"
    );
    for worker in persistent_workers.values() {
        let env = util::env_map(worker["spec"]["template"]["spec"]["containers"][0].get("env"));
        ensure!(
            !env.contains_key("DUCKFLEET_HISTORY_RETENTION_DAYS"),
            "retention belongs to gateway maintenance"
        );
        ensure!(
            env["DUCKFLEET_PLATFORM_WAIT_SECONDS"]
                == gateway_env["DUCKFLEET_PLATFORM_WAIT_SECONDS"],
            "platform wait differs between worker and gateway"
        );
        ensure!(
            env["DUCKFLEET_PLATFORM_WAIT_SECONDS"].parse::<u64>()? <= 600,
            "platform wait exceeds its supported bound"
        );
    }

    let bundled_parts = [
        ("StatefulSet", "duckfleet-postgres"),
        ("Service", "postgres"),
        ("ConfigMap", "duckfleet-postgres-init"),
    ];
    let present: Vec<_> = bundled_parts
        .iter()
        .filter(|(kind, name)| by_name.contains_key(&(kind.to_string(), name.to_string())))
        .collect();
    ensure!(
        present.is_empty() || present.len() == bundled_parts.len(),
        "partial bundled postgres render"
    );

    let ingresses: Vec<_> = objects
        .iter()
        .filter(|object| object["kind"] == "Ingress")
        .collect();
    let exposure_policy = by_name.get(&("NetworkPolicy".into(), "duckfleet-exposure".into()));
    if ingresses.is_empty() {
        ensure!(exposure_policy.is_none(), "exposure policy without Ingress");
    } else {
        ensure!(
            ingresses.len() == 1 && exposure_policy.is_some(),
            "one Ingress and its policy are required"
        );
        let ingress = ingresses[0];
        let tls = util::array(ingress["spec"].get("tls"));
        let tls_hosts: BTreeSet<_> = tls
            .iter()
            .flat_map(|block| util::array(block.get("hosts")))
            .filter_map(Value::as_str)
            .collect();
        ensure!(
            tls.iter().all(|block| block.get("secretName").is_some()),
            "TLS without a certificate secret"
        );
        let rules = util::array(ingress["spec"].get("rules"));
        let rule_hosts: BTreeSet<_> = rules
            .iter()
            .filter_map(|rule| rule["host"].as_str())
            .collect();
        ensure!(
            !rule_hosts.is_empty() && rule_hosts.is_subset(&tls_hosts),
            "every exposed host must terminate TLS"
        );
        ensure!(
            ingress["metadata"]["annotations"]["nginx.ingress.kubernetes.io/ssl-redirect"]
                == "true",
            "plain HTTP must redirect to TLS"
        );
        for rule in rules {
            for path in util::array(rule["http"].get("paths")) {
                ensure!(
                    path["backend"]["service"]["name"] == "server"
                        && path["backend"]["service"]["port"]["number"] == 8080,
                    "exposure may point only at gateway"
                );
            }
        }
        let admitted = util::array(exposure_policy.unwrap()["spec"]["ingress"][0].get("from"));
        ensure!(
            !admitted.is_empty()
                && admitted
                    .iter()
                    .all(|source| source.get("namespaceSelector").is_some()),
            "exposure must admit only the named controller namespace"
        );
    }

    let isolation = &by_name
        .get(&("NetworkPolicy".into(), "duckfleet-isolation".into()))
        .context("missing isolation policy")?["spec"];
    ensure!(
        isolation["policyTypes"] == json!(["Ingress", "Egress"]),
        "isolation policy directions changed"
    );
    let dns_rules: Vec<_> = util::array(isolation.get("egress"))
        .iter()
        .filter(|rule| {
            util::array(rule.get("ports"))
                .iter()
                .any(|port| port["port"] == 53)
        })
        .collect();
    ensure!(!dns_rules.is_empty(), "no DNS egress rule");
    let mut address_dns = false;
    for rule in dns_rules {
        let ports: BTreeSet<_> = util::array(rule.get("ports"))
            .iter()
            .filter_map(|port| port["port"].as_u64())
            .collect();
        ensure!(
            ports == BTreeSet::from([53]),
            "DNS rule must contain only port 53"
        );
        for peer in util::array(rule.get("to")) {
            if let Some(cidr) = peer["ipBlock"]["cidr"].as_str() {
                address_dns = true;
                let network = Network::parse(cidr)?;
                ensure!(
                    network.is_private() && network.prefix_len() >= 8,
                    "DNS egress must be a bounded private range: {cidr}"
                );
            }
        }
    }
    ensure!(address_dns, "DNS must also be reachable by address");

    let isolated_keys = [
        ("NetworkPolicy", "duckfleet-gateway-kubernetes"),
        ("ConfigMap", "duckfleet-isolated-pod"),
    ];
    let present: Vec<_> = isolated_keys
        .iter()
        .filter(|(kind, name)| by_name.contains_key(&(kind.to_string(), name.to_string())))
        .collect();
    ensure!(
        present.is_empty() || present.len() == isolated_keys.len(),
        "isolated compute is half-rendered"
    );
    ensure!(
        by_name[&("ServiceAccount".into(), "duckfleet-gateway".into())]["automountServiceAccountToken"]
            == true,
        "gateway account must mount its token"
    );
    for account in ["duckfleet-worker", "duckfleet-no-aws", "duckfleet-results"] {
        ensure!(
            by_name[&("ServiceAccount".into(), account.into())]["automountServiceAccountToken"]
                == false,
            "{account} must not mount a token"
        );
    }
    if present.is_empty() {
        let expected = if gateway_env.contains_key("DUCKFLEET_RESULT_DATA_PATH") {
            "duckfleet-results"
        } else {
            "duckfleet-no-aws"
        };
        ensure!(
            gateway_pod["serviceAccountName"] == expected,
            "default gateway identity changed"
        );
        let volumes: BTreeSet<_> = util::array(gateway_pod.get("volumes"))
            .iter()
            .filter_map(|volume| volume["name"].as_str())
            .collect();
        ensure!(
            volumes.is_subset(&BTreeSet::from(["persistent-pools"])),
            "default gateway has unexpected volumes"
        );
    } else {
        ensure!(
            gateway_pod["serviceAccountName"] == "duckfleet-gateway",
            "isolated gateway identity changed"
        );
        let api = &by_name[&(
            "NetworkPolicy".into(),
            "duckfleet-gateway-kubernetes".into(),
        )]["spec"];
        ensure!(
            api["podSelector"]["matchLabels"] == json!({"app":"duckfleet-gateway"})
                && api["policyTypes"] == json!(["Egress"]),
            "API policy target changed"
        );
        for rule in util::array(api.get("egress")) {
            let ports: BTreeSet<_> = util::array(rule.get("ports"))
                .iter()
                .filter_map(|port| port["port"].as_u64())
                .collect();
            ensure!(
                !ports.is_empty() && ports.is_subset(&BTreeSet::from([443, 6443])),
                "API egress ports are too broad"
            );
            for peer in util::array(rule.get("to")) {
                let cidr = util::text(peer["ipBlock"].get("cidr"));
                let network = Network::parse(cidr)?;
                ensure!(
                    network.is_private() && network.prefix_len() >= 8,
                    "API egress must be a bounded private range"
                );
            }
        }
        let template = util::read_json_from_string(util::text(
            by_name[&("ConfigMap".into(), "duckfleet-isolated-pod".into())]["data"].get("pod.json"),
        ))?;
        let persistent = persistent_workers
            .values()
            .next()
            .map_or(&server["spec"]["template"]["spec"], |worker| {
                &worker["spec"]["template"]["spec"]
            });
        let isolated = &template["spec"];
        let container = &isolated["containers"][0];
        ensure!(
            isolated["securityContext"] == persistent["securityContext"]
                && container["securityContext"] == persistent["containers"][0]["securityContext"],
            "isolated security context differs"
        );
        ensure!(
            container["image"] == persistent["containers"][0]["image"],
            "isolated image differs"
        );
        ensure!(
            isolated["nodeSelector"] == persistent["nodeSelector"]
                && isolated.get("tolerations") == persistent.get("tolerations"),
            "isolated placement differs"
        );
        ensure!(
            isolated["serviceAccountName"] == "duckfleet-worker"
                && isolated["automountServiceAccountToken"] == false,
            "isolated identity is too broad"
        );
        ensure!(
            template["metadata"]["labels"]["app"] == "duckfleet-worker"
                && template["metadata"]["labels"]["duckfleet-compute"] == "isolated",
            "isolated labels are required"
        );
        ensure!(
            isolated["restartPolicy"] == "Never"
                && isolated["activeDeadlineSeconds"].as_u64().unwrap_or(0) >= 60,
            "isolated lifecycle is unbounded"
        );
        for quantity in ["cpu", "memory"] {
            ensure!(
                container["resources"]["requests"][quantity]
                    == container["resources"]["limits"][quantity],
                "isolated {quantity} requests must equal limits"
            );
        }
        let declared = util::env_map(container.get("env"));
        for variable in [
            "DUCKFLEET_WORKER_ID",
            "DUCKFLEET_WORKER_CPU",
            "DUCKFLEET_WORKER_MEMORY",
            "DUCKFLEET_WORKER_DISK",
        ] {
            ensure!(
                declared.contains_key(variable),
                "isolated pod lacks {variable}"
            );
        }
        ensure!(
            declared["DUCKFLEET_ISOLATED"] == "true"
                && !declared.contains_key("DUCKFLEET_POOL")
                && declared["DUCKFLEET_WORKER_MAX_SESSIONS"] == "1",
            "isolated session identity changed"
        );
        ensure!(
            declared["S3_AUTH_MODE"] == "pod_identity"
                && declared["AWS_EC2_METADATA_DISABLED"] == "true"
                && !declared.contains_key("S3_SECRET_KEY"),
            "isolated S3 identity changed"
        );
        let secrets = secret_keys(container)?;
        for key in ["CATALOG_DSN", "PLATFORM_DSN", "DUCKFLEET_INTERNAL_TOKEN"] {
            ensure!(secrets.contains_key(key), "isolated pod lacks {key}");
        }
    }

    for worker in persistent_workers.values() {
        let pod = &worker["spec"]["template"]["spec"];
        let keys = secret_keys(&pod["containers"][0])?;
        ensure!(
            keys["PLATFORM_DSN"] == "PLATFORM_WORKER_DSN"
                && keys["DUCKFLEET_INTERNAL_TOKEN"] == "DUCKFLEET_INTERNAL_TOKEN",
            "worker secret keys changed"
        );
        ensure!(
            !keys.contains_key("DUCKFLEET_TOKEN")
                && !keys.contains_key("DUCKFLEET_MAINTENANCE_DSN"),
            "worker has privileged sibling credentials"
        );
        ensure!(
            util::array(pod.get("volumes"))
                .iter()
                .all(|volume| !util::object(volume)
                    .keys()
                    .any(|key| key.contains("secret"))),
            "worker must not mount the whole Secret"
        );
    }
    let gateway_keys = secret_keys(gateway)?;
    ensure!(
        gateway_keys["PLATFORM_DSN"] == "PLATFORM_GATEWAY_DSN"
            && gateway_keys["DUCKFLEET_MAINTENANCE_DSN"] == "PLATFORM_MAINTENANCE_DSN",
        "gateway secret keys changed"
    );
    ensure!(
        gateway_keys["DUCKFLEET_TOKEN"] != gateway_keys["DUCKFLEET_INTERNAL_TOKEN"],
        "public and internal token keys must differ"
    );
    if let Some(migration) = migrations.first() {
        ensure!(
            secret_keys(&migration["spec"]["template"]["spec"]["containers"][0])?
                == BTreeMap::from([("PLATFORM_DSN".into(), "PLATFORM_MIGRATION_DSN".into())]),
            "migration has extra Secret keys"
        );
    }

    if let Some(result_path) = gateway_env.get("DUCKFLEET_RESULT_DATA_PATH") {
        ensure!(
            gateway_env["DUCKFLEET_RESULT_AUTH_MODE"] == "pod_identity"
                && gateway_env["DUCKFLEET_RESULT_ALLOW_HTTP"] == "false"
                && gateway_env["AWS_EC2_METADATA_DISABLED"] == "true",
            "result storage identity changed"
        );
        let lake = gateway_env["DUCKFLEET_RESULT_LAKE_DATA_PATH"].trim_end_matches('/');
        let result = result_path.trim_end_matches('/');
        ensure!(
            lake != result
                && !lake.starts_with(&format!("{result}/"))
                && !result.starts_with(&format!("{lake}/")),
            "result and lake prefixes overlap"
        );
        ensure!(
            !gateway_env.contains_key("S3_ACCESS_KEY")
                && !gateway_env.contains_key("S3_SECRET_KEY")
                && !gateway_env.contains_key("DUCKFLEET_RESULT_SECRET_KEY"),
            "gateway has static storage credentials"
        );
        ensure!(
            by_name.contains_key(&("NetworkPolicy".into(), "duckfleet-gateway-results".into())),
            "result egress policy is missing"
        );
        for worker in persistent_workers.values() {
            let env = util::env_map(worker["spec"]["template"]["spec"]["containers"][0].get("env"));
            ensure!(
                env["DUCKFLEET_RESULT_DATA_PATH"] == *result_path
                    && env["DUCKFLEET_RESULT_AUTH_MODE"] == "pod_identity",
                "worker result storage differs from gateway"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_and_secret_key_guards_reject_broadening() {
        admission_limits(
            &json!({"max_queued_queries":32,"max_queued_per_principal":16}),
            None,
        )
        .unwrap();
        assert!(admission_limits(&json!({"max_queued_queries":0}), None).is_err());
        let mut container = json!({"env":[{"name":"PLATFORM_DSN","valueFrom":{"secretKeyRef":{"key":"PLATFORM_WORKER_DSN"}}}]});
        assert_eq!(
            secret_keys(&container).unwrap()["PLATFORM_DSN"],
            "PLATFORM_WORKER_DSN"
        );
        container["envFrom"] = json!([{"secretRef":{"name":"all"}}]);
        assert!(secret_keys(&container).is_err());
    }
}
